# -*- coding: utf-8 -*-
# Copyright 2014-2016 OpenMarket Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ._base import SQLBaseStore
from synapse.api.constants import PresenceState
from synapse.util.caches.descriptors import cached, cachedInlineCallbacks

from collections import namedtuple
from twisted.internet import defer


class UserPresenceState(namedtuple("UserPresenceState",
                        ("user_id", "state", "last_active", "last_federation_update",
                            "last_user_sync", "status_msg"))):
    """Represents the current presence state of the user.

    user_id (str)
    last_active (int): Time in msec that the user last interacted with server.
    last_federation_update (int): Time in msec since either a) we sent a presence
        update to other servers or b) we received a presence update, depending
        on if is a local user or not.
    last_user_sync (int): Time in msec that the user last *completed* a sync
        (or event stream).
    status_msg (str): User set status message.
    """

    def copy_and_replace(self, **kwargs):
        return self._replace(**kwargs)

    @classmethod
    def default(cls, user_id):
        """Returns a default presence state.
        """
        return cls(
            user_id=user_id,
            state=PresenceState.OFFLINE,
            last_active=0,
            last_federation_update=0,
            last_user_sync=0,
            status_msg=None,
        )


class PresenceStore(SQLBaseStore):
    @defer.inlineCallbacks
    def update_presence(self, presence_states):
        stream_id_manager = yield self._presence_id_gen.get_next(self)
        with stream_id_manager as stream_id:
            yield self.runInteraction(
                "update_presence",
                self._update_presence_txn, stream_id, presence_states,
            )

        defer.returnValue((stream_id, self._presence_id_gen.get_max_token()))

    def _update_presence_txn(self, txn, stream_id, presence_states):
        for state in presence_states:
            txn.call_after(
                self.presence_stream_cache.entity_has_changed,
                state.user_id, stream_id,
            )

        # Actually insert new rows
        self._simple_insert_many_txn(
            txn,
            table="presence_stream",
            values=[
                {
                    "stream_id": stream_id,
                    "user_id": state.user_id,
                    "state": state.state,
                    "last_active": state.last_active,
                    "last_federation_update": state.last_federation_update,
                    "last_user_sync": state.last_user_sync,
                    "status_msg": state.status_msg,
                }
                for state in presence_states
            ],
        )

        # Delete old rows to stop database from getting really big
        sql = (
            "DELETE FROM presence_stream WHERE"
            " stream_id < ?"
            " AND user_id IN (%s)"
        )

        batches = (
            presence_states[i:i + 50]
            for i in xrange(0, len(presence_states), 50)
        )
        for states in batches:
            args = [stream_id]
            args.extend(s.user_id for s in states)
            txn.execute(
                sql % (",".join("?" for _ in states),),
                args
            )

    @defer.inlineCallbacks
    def get_presence_for_users(self, user_ids):
        rows = yield self._simple_select_many_batch(
            table="presence_stream",
            column="user_id",
            iterable=user_ids,
            keyvalues={},
            retcols=(
                "user_id",
                "state",
                "last_active",
                "last_federation_update",
                "last_user_sync",
                "status_msg",
            ),
        )

        defer.returnValue([UserPresenceState(**row) for row in rows])

    def get_current_presence_token(self):
        return self._presence_id_gen.get_max_token()

    def allow_presence_visible(self, observed_localpart, observer_userid):
        return self._simple_insert(
            table="presence_allow_inbound",
            values={"observed_user_id": observed_localpart,
                    "observer_user_id": observer_userid},
            desc="allow_presence_visible",
            or_ignore=True,
        )

    def disallow_presence_visible(self, observed_localpart, observer_userid):
        return self._simple_delete_one(
            table="presence_allow_inbound",
            keyvalues={"observed_user_id": observed_localpart,
                       "observer_user_id": observer_userid},
            desc="disallow_presence_visible",
        )

    def is_presence_visible(self, observed_localpart, observer_userid):
        return self._simple_select_one(
            table="presence_allow_inbound",
            keyvalues={"observed_user_id": observed_localpart,
                       "observer_user_id": observer_userid},
            retcols=["observed_user_id"],
            allow_none=True,
            desc="is_presence_visible",
        )

    def add_presence_list_pending(self, observer_localpart, observed_userid):
        return self._simple_insert(
            table="presence_list",
            values={"user_id": observer_localpart,
                    "observed_user_id": observed_userid,
                    "accepted": False},
            desc="add_presence_list_pending",
        )

    @defer.inlineCallbacks
    def set_presence_list_accepted(self, observer_localpart, observed_userid):
        result = yield self._simple_update_one(
            table="presence_list",
            keyvalues={"user_id": observer_localpart,
                       "observed_user_id": observed_userid},
            updatevalues={"accepted": True},
            desc="set_presence_list_accepted",
        )
        self.get_presence_list_accepted.invalidate((observer_localpart,))
        self.get_presence_list_observers_accepted.invalidate((observed_userid,))
        defer.returnValue(result)

    def get_presence_list(self, observer_localpart, accepted=None):
        if accepted:
            return self.get_presence_list_accepted(observer_localpart)
        else:
            keyvalues = {"user_id": observer_localpart}
            if accepted is not None:
                keyvalues["accepted"] = accepted

            return self._simple_select_list(
                table="presence_list",
                keyvalues=keyvalues,
                retcols=["observed_user_id", "accepted"],
                desc="get_presence_list",
            )

    @cached()
    def get_presence_list_accepted(self, observer_localpart):
        return self._simple_select_list(
            table="presence_list",
            keyvalues={"user_id": observer_localpart, "accepted": True},
            retcols=["observed_user_id", "accepted"],
            desc="get_presence_list_accepted",
        )

    @cachedInlineCallbacks()
    def get_presence_list_observers_accepted(self, observed_userid):
        user_localparts = yield self._simple_select_onecol(
            table="presence_list",
            keyvalues={"observed_user_id": observed_userid, "accepted": True},
            retcol="user_id",
            desc="get_presence_list_accepted",
        )

        defer.returnValue([
            "@%s:%s" % (u, self.hs.hostname,) for u in user_localparts
        ])

    @defer.inlineCallbacks
    def del_presence_list(self, observer_localpart, observed_userid):
        yield self._simple_delete_one(
            table="presence_list",
            keyvalues={"user_id": observer_localpart,
                       "observed_user_id": observed_userid},
            desc="del_presence_list",
        )
        self.get_presence_list_accepted.invalidate((observer_localpart,))
        self.get_presence_list_observers_accepted.invalidate((observed_userid,))
