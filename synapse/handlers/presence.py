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

from twisted.internet import defer, reactor
from contextlib import contextmanager

from synapse.api.errors import SynapseError, AuthError
from synapse.api.constants import PresenceState
from synapse.storage.presence import UserPresenceState

from synapse.util.logcontext import preserve_fn
from synapse.util.logutils import log_function
from synapse.util.wheel_timer import WheelTimer
from synapse.types import UserID
import synapse.metrics

from ._base import BaseHandler

import logging


logger = logging.getLogger(__name__)

metrics = synapse.metrics.get_metrics_for(__name__)


# Don't bother bumping "last active" time if it differs by less than 60 seconds
LAST_ACTIVE_GRANULARITY = 60 * 1000
SYNC_ONLINE_TIMEOUT = 30 * 1000
IDLE_TIMER = 5 * 60 * 1000
FEDERATION_TIMEOUT = 30 * 60 * 1000
FEDERATION_PING_INTERVAL = 20 * 60 * 1000

assert LAST_ACTIVE_GRANULARITY < IDLE_TIMER


def user_presence_changed(distributor, user, statuscache):
    return distributor.fire("user_presence_changed", user, statuscache)


def collect_presencelike_data(distributor, user, content):
    return distributor.fire("collect_presencelike_data", user, content)


class PresenceHandler(BaseHandler):

    def __init__(self, hs):
        super(PresenceHandler, self).__init__(hs)
        self.hs = hs
        self.clock = hs.get_clock()
        self.store = hs.get_datastore()
        self.wheel_timer = WheelTimer()
        self.notifier = hs.get_notifier()
        self.federation = hs.get_replication_layer()

        self.federation.register_edu_handler(
            "m.presence", self.incoming_presence
        )
        self.federation.register_edu_handler(
            "m.presence_invite",
            lambda origin, content: self.invite_presence(
                observed_user=UserID.from_string(content["observed_user"]),
                observer_user=UserID.from_string(content["observer_user"]),
            )
        )
        self.federation.register_edu_handler(
            "m.presence_accept",
            lambda origin, content: self.accept_presence(
                observed_user=UserID.from_string(content["observed_user"]),
                observer_user=UserID.from_string(content["observer_user"]),
            )
        )
        self.federation.register_edu_handler(
            "m.presence_deny",
            lambda origin, content: self.deny_presence(
                observed_user=UserID.from_string(content["observed_user"]),
                observer_user=UserID.from_string(content["observer_user"]),
            )
        )

        distributor = hs.get_distributor()
        distributor.observe("user_joined_room", self.user_joined_room)

        active_presence = self.store.take_presence_startup_info()

        self.user_to_current_state = {
            state.user_id: state
            for state in active_presence
        }

        now = self.clock.time_msec()
        for state in active_presence:
            self.wheel_timer.insert(
                now=now,
                obj=state.user_id,
                then=state.last_active + IDLE_TIMER,
            )
            self.wheel_timer.insert(
                now=now,
                obj=state.user_id,
                then=state.last_user_sync + SYNC_ONLINE_TIMEOUT,
            )
            if self.hs.is_mine_id(state.user_id):
                self.wheel_timer.insert(
                    now=now,
                    obj=state.user_id,
                    then=state.last_federation_update + FEDERATION_PING_INTERVAL,
                )
            else:
                self.wheel_timer.insert(
                    now=now,
                    obj=state.user_id,
                    then=state.last_federation_update + FEDERATION_TIMEOUT,
                )

        self.unpersisted_users_changes = set()

        reactor.addSystemEventTrigger("before", "shutdown", self._on_shutdown)

        self.serial_to_user = {}
        self._next_serial = 1

        # Keeps track of the number of *ongoing* syncs. While this is non zero
        # a user will never go offline.
        self.user_to_num_current_syncs = {}

        # Start a LoopingCall in 30s that fires every 5s.
        # The initial delay is to allow disconnected clients a chance to
        # reconnect before we treat them as offline.
        self.clock.call_later(
            0 * 1000,
            self.clock.looping_call,
            self._handle_timeouts,
            5000,
        )

    @defer.inlineCallbacks
    def _on_shutdown(self):
        logger.info(
            "Performing _on_shutdown. Persiting %d unpersisted changes",
            len(self.user_to_current_state)
        )

        if self.unpersisted_users_changes:
            yield self.store.update_presence([
                self.user_to_current_state[user_id]
                for user_id in self.unpersisted_users_changes
            ])
        logger.info("Finished _on_shutdown")

    @defer.inlineCallbacks
    def _update_states(self, new_states):
        """Updates presence of users. Sets the appropriate timeouts. Pokes
        the notifier and federation if and only if the changed presence state
        should be sent to clients/servers.
        """
        now = self.clock.time_msec()

        to_notify = {}  # Changes we want to notify everyone about
        to_federation_ping = {}  # These need sending keep-alives
        for new_state in new_states:
            logger.info("new_state: %r, new_states: %r", new_state, new_states)
            user_id = new_state.user_id
            prev_state = self.current_state_for_user(user_id)
            self.user_to_current_state[user_id] = new_state

            # If the users are ours then we want to set up a bunch of timers
            # to time things out.
            if self.hs.is_mine_id(user_id):
                if new_state.state == PresenceState.ONLINE:
                    # Idle timer
                    self.wheel_timer.insert(
                        now=now,
                        obj=user_id,
                        then=new_state.last_active + IDLE_TIMER
                    )

                if new_state.state != PresenceState.OFFLINE:
                    # User has stopped syncing
                    self.wheel_timer.insert(
                        now=now,
                        obj=user_id,
                        then=new_state.last_user_sync + SYNC_ONLINE_TIMEOUT
                    )

                    last_federate = new_state.last_federation_update
                    if now - last_federate > FEDERATION_PING_INTERVAL:
                        # Been a while since we've poked remote servers
                        new_state = new_state.copy_and_replace(
                            last_federation_update=now,
                        )
                        to_federation_ping[user_id] = new_state

            # Check whether the change was something worth notifying about
            if should_notify(prev_state, new_state):
                logger.info("new_state should_notify: %r!", new_state)
                new_state.copy_and_replace(
                    last_federation_update=now,
                )
                to_notify[user_id] = new_state

        if to_notify:
            yield self._persist_and_notify(to_notify.values())

        self.unpersisted_users_changes |= set(s.user_id for s in new_states)
        self.unpersisted_users_changes -= set(to_notify.keys())

        to_federation_ping = {
            user_id: state for user_id, state in to_federation_ping.items()
            if user_id not in to_notify
        }
        if to_federation_ping:
            _, _, hosts_to_states = yield self._get_interested_parties(
                to_federation_ping.values()
            )

            self._push_to_remotes(hosts_to_states)

    def _handle_timeouts(self):
        """Checks the presence of users that have timed out and updates as
        appropriate.
        """
        now = self.clock.time_msec()

        # Fetch the list of users that *may* have timed out. Things may have
        # changed since the timeout was set, so we won't necessarily have to
        # take any action.
        users_to_check = self.wheel_timer.fetch(now)

        changes = {}  # Actual changes we need to notify people about

        for user_id in set(users_to_check):
            state = self.user_to_current_state.get(user_id, None)
            if not state:
                continue

            if self.hs.is_mine_id(user_id):
                if state.state == PresenceState.OFFLINE:
                    continue

                if state.state == PresenceState.ONLINE:
                    if now - state.last_active > IDLE_TIMER:
                        # Currently online, but last activity ages ago so auto
                        # idle
                        changes[user_id] = state.copy_and_replace(
                            state=PresenceState.UNAVAILABLE,
                        )

                if now - state.last_federation_update > FEDERATION_PING_INTERVAL:
                    # Need to send ping to other servers to ensure they don't
                    # timeout and set us to offline
                    changes[user_id] = state

                # If there are have been no sync for a while (and none ongoing),
                # set presence to offline
                if not self.user_to_num_current_syncs.get(user_id, 0):
                    if now - state.last_user_sync > SYNC_ONLINE_TIMEOUT:
                        logger.info(
                            "Setting %r to OFFLINE since %d - %d > %d",
                            state.user_id,
                            now, state.last_user_sync, SYNC_ONLINE_TIMEOUT
                        )
                        changes[user_id] = state.copy_and_replace(
                            state=PresenceState.OFFLINE,
                        )
            else:
                # We expect to be poked occaisonally by the other side.
                # This is to protect against forgetful/buggy servers, so that
                # no one gets stuck online forever.
                if now - state.last_federation_update > FEDERATION_TIMEOUT:
                    if state.state != PresenceState.OFFLINE:
                        # The other side seems to have disappeared.
                        changes[user_id] = state.copy_and_replace(
                            state=PresenceState.OFFLINE,
                        )

        preserve_fn(self._update_states)(changes.values())

    @defer.inlineCallbacks
    def bump_presence_active_time(self, user):
        user_id = user.to_string()

        logger.info("Bumping presence %r", user_id)

        prev_state = self.current_state_for_user(user_id)

        yield self._update_states([prev_state.copy_and_replace(
            state=PresenceState.ONLINE,
            last_active=self.clock.time_msec(),
        )])

    @defer.inlineCallbacks
    def user_syncing(self, user_id, timeout=0, affect_presence=True):
        logger.info("Before user_syncing")
        if affect_presence:
            curr_sync = self.user_to_num_current_syncs.get(user_id, 0)
            self.user_to_num_current_syncs[user_id] = curr_sync + 1

            prev_state = self.current_state_for_user(user_id)
            if prev_state.state == PresenceState.OFFLINE:
                yield self._update_states([prev_state.copy_and_replace(
                    state=PresenceState.ONLINE,
                    last_active=self.clock.time_msec(),
                    last_user_sync=self.clock.time_msec() + timeout,
                )])
            else:
                yield self._update_states([prev_state.copy_and_replace(
                    last_user_sync=self.clock.time_msec() + timeout,
                )])

        logger.info("After user_syncing")

        @contextmanager
        def _user_syncing():
            try:
                yield
            finally:
                if affect_presence:
                    self.user_to_num_current_syncs[user_id] -= 1

                    prev_state = self.current_state_for_user(user_id)
                    preserve_fn(self._update_states)([prev_state.copy_and_replace(
                        last_user_sync=self.clock.time_msec(),
                    )])

        defer.returnValue(_user_syncing())

    def current_state_for_user(self, user_id):
        logger.info("current_state_for_user %r", self.user_to_current_state)
        return self.user_to_current_state.get(
            user_id, UserPresenceState.default(user_id)
        )

    @defer.inlineCallbacks
    def _get_interested_parties(self, states):
        room_ids_to_states = {}
        users_to_states = {}
        for state in states:
            events = yield self.store.get_rooms_for_user(state.user_id)
            for e in events:
                room_ids_to_states.setdefault(e.room_id, []).append(state)

            plist = yield self.store.get_presence_list_observers_accepted(state.user_id)
            for u in plist:
                users_to_states.setdefault(u, []).append(state)

            # Always notify self
            users_to_states.setdefault(state.user_id, []).append(state)

        hosts_to_states = {}
        for room_id, states in room_ids_to_states.items():
            hosts = yield self.store.get_joined_hosts_for_room(room_id)
            for host in hosts:
                hosts_to_states.setdefault(host, []).extend(states)

        for user_id, states in users_to_states.items():
            host = UserID.from_string(user_id).domain
            hosts_to_states.setdefault(host, []).extend(states)

        # TODO: de-dup hosts_to_states, as a single host might have multiple
        # of same presence

        defer.returnValue((room_ids_to_states, users_to_states, hosts_to_states))

    @defer.inlineCallbacks
    def _persist_and_notify(self, states):
        logger.info("token before presence %r", self.store.get_current_presence_token())
        stream_id, max_token = yield self.store.update_presence(states)
        logger.info("token after presence %r %r", self.store.get_current_presence_token(), stream_id)

        parties = yield self._get_interested_parties(states)
        room_ids_to_states, users_to_states, hosts_to_states = parties

        logger.info("Notifying rooms %r, users %r", room_ids_to_states.keys(), users_to_states.keys())

        self.notifier.on_new_event(
            "presence_key", stream_id, rooms=room_ids_to_states.keys(),
            users=[UserID.from_string(u) for u in users_to_states.keys()]
        )

        self._push_to_remotes(hosts_to_states)

    def _push_to_remotes(self, hosts_to_states):
        now = self.clock.time_msec()
        for host, states in hosts_to_states.items():
            self.federation.send_edu(
                destination=host,
                edu_type="m.presence",
                content={
                    "push": [
                        _format_user_presence_state(state, now)
                        for state in states
                    ]
                }
            )

    @defer.inlineCallbacks
    def incoming_presence(self, origin, content):
        now = self.clock.time_msec()
        updates = []
        for push in content.get("push", []):
            user_id = push.get("user_id", None)
            if not user_id:
                logger.info(
                    "Got presence update from %r with no 'user_id': %r",
                    origin, push,
                )
                continue

            presence_state = push.get("presence", None)
            if not presence_state:
                logger.info(
                    "Got presence update from %r with no 'presence_state': %r",
                    origin, push,
                )
                continue

            new_fields = {
                "state": presence_state,
                "last_federation_update": now,
            }

            last_active_ago = push.get("last_active_ago", None)
            if last_active_ago is not None:
                new_fields["last_active"] = now - last_active_ago

            new_fields["status_msg"] = push.get("status_msg", None)

            logger.info("Updating %r with %r", user_id, new_fields)

            prev_state = self.current_state_for_user(user_id)
            updates.append(prev_state.copy_and_replace(**new_fields))

        if updates:
            yield self._update_states(updates)

    @defer.inlineCallbacks
    def get_state(self, target_user, auth_user, as_event=False, check_auth=True):
        results = yield self.get_states(
            [target_user.to_string()], auth_user,
            as_event=as_event,
            check_auth=check_auth,
        )

        defer.returnValue(results[0])

    @defer.inlineCallbacks
    def get_states(self, target_user_ids, auth_user, as_event=False, check_auth=True):
        # TODO: Check auth

        updates = []
        missing = []
        for user_id in target_user_ids:
            state = self.user_to_current_state.get(user_id, None)
            if state is None:
                missing.append(user_id)
            else:
                updates.append(state)

        missing_updates = yield self.store.get_presence_for_users(missing)
        updates.extend(missing_updates)

        for user_id in set(target_user_ids) - set(u.user_id for u in updates):
            updates.append(UserPresenceState.default(user_id))

        now = self.clock.time_msec()
        if as_event:
            defer.returnValue([
                {
                    "type": "m.presence",
                    "content": _format_user_presence_state(state, now),
                }
                for state in updates
            ])
        else:
            defer.returnValue([
                _format_user_presence_state(state, now) for state in updates
            ])

    @defer.inlineCallbacks
    def set_state(self, target_user, auth_user, state):
        # TODO: Auth

        logger.info("set_state")

        status_msg = state.get("status_msg", None)
        presence = state["presence"]

        user_id = target_user.to_string()

        prev_state = self.current_state_for_user(user_id)

        new_fields = {
            "state": presence,
            "status_msg": status_msg
        }

        if presence == PresenceState.ONLINE:
            new_fields["last_active"] = self.clock.time_msec()

        yield self._update_states([prev_state.copy_and_replace(**new_fields)])

    @defer.inlineCallbacks
    def user_joined_room(self, user, room_id):
        # We only need to send presence to servers that don't have it yet. We
        # don't need to send to local clients here, as that is done as part
        # of the event stream/sync.
        # TODO: Only send to servers not already in the room.
        if self.hs.is_mine(user):
            hosts = yield self.store.get_joined_hosts_for_room(room_id)

            # We only query in memory database, as the only thing that won't
            # be in there are offline presence states, and we don't care that
            # much if last active time is missing for these users.
            state = self.current_state_for_user(user.to_string())

            self._push_to_remotes({host: (state,) for host in hosts})
        else:
            user_ids = yield self.store.get_users_in_room(room_id)
            user_ids = filter(self.hs.is_mine_id, user_ids)

            states = (self.current_state_for_user(u) for u in user_ids)

            self._push_to_remotes({user.domain: states})

    @defer.inlineCallbacks
    def get_presence_list(self, observer_user, accepted=None):
        if not self.hs.is_mine(observer_user):
            raise SynapseError(400, "User is not hosted on this Home Server")

        presence_list = yield self.store.get_presence_list(
            observer_user.localpart, accepted=accepted
        )

        results = yield self.get_states(
            target_user_ids=[row["observed_user_id"] for row in presence_list],
            auth_user=observer_user,
            check_auth=False,
            as_event=False,
        )

        is_accepted = {
            row["observed_user_id"]: row["accepted"] for row in presence_list
        }

        for result in results:
            result.update({
                "accepted": is_accepted,
            })

        defer.returnValue(results)

    @defer.inlineCallbacks
    def send_presence_invite(self, observer_user, observed_user):
        # TODO AUTH

        if not self.hs.is_mine(observer_user):
            raise SynapseError(400, "User is not hosted on this Home Server")

        yield self.store.add_presence_list_pending(
            observer_user.localpart, observed_user.to_string()
        )

        if self.hs.is_mine(observed_user):
            yield self.invite_presence(observed_user, observer_user)
        else:
            yield self.federation.send_edu(
                destination=observed_user.domain,
                edu_type="m.presence_invite",
                content={
                    "observed_user": observed_user.to_string(),
                    "observer_user": observer_user.to_string(),
                }
            )

    @defer.inlineCallbacks
    def invite_presence(self, observed_user, observer_user):
        if not self.hs.is_mine(observed_user):
            raise SynapseError(400, "User is not hosted on this Home Server")

        # TODO: Don't auto accept
        if self.hs.is_mine(observer_user):
            yield self.accept_presence(observed_user, observer_user)
        else:
            self.federation.send_edu(
                destination=observer_user.domain,
                edu_type="m.presence_accept",
                content={
                    "observed_user": observed_user.to_string(),
                    "observer_user": observer_user.to_string(),
                }
            )

            state_dict = yield self.get_state(
                observed_user, observer_user,
                check_auth=None, as_event=False
            )

            self.federation.send_edu(
                destination=observer_user.domain,
                edu_type="m.presence",
                content={
                    "push": [state_dict]
                }
            )

    @defer.inlineCallbacks
    def accept_presence(self, observed_user, observer_user):
        """Handles a m.presence_accept EDU. Mark a presence invite from a
        local or remote user as accepted in a local user's presence list.
        Starts polling for presence updates from the local or remote user.
        Args:
            observed_user(UserID): The user to update in the presence list.
            observer_user(UserID): The owner of the presence list to update.
        """
        yield self.store.set_presence_list_accepted(
            observer_user.localpart, observed_user.to_string()
        )

    @defer.inlineCallbacks
    def deny_presence(self, observed_user, observer_user):
        """Handle a m.presence_deny EDU. Removes a local or remote user from a
        local user's presence list.
        Args:
            observed_user(UserID): The local or remote user to remove from the
                list.
            observer_user(UserID): The local owner of the presence list.
        Returns:
            A Deferred.
        """
        yield self.store.del_presence_list(
            observer_user.localpart, observed_user.to_string()
        )

        # TODO(paul): Inform the user somehow?

    @defer.inlineCallbacks
    def drop(self, observed_user, observer_user):
        """Remove a local or remote user from a local user's presence list and
        unsubscribe the local user from updates that user.
        Args:
            observed_user(UserId): The local or remote user to remove from the
                list.
            observer_user(UserId): The local owner of the presence list.
        Returns:
            A Deferred.
        """
        if not self.hs.is_mine(observer_user):
            raise SynapseError(400, "User is not hosted on this Home Server")

        yield self.store.del_presence_list(
            observer_user.localpart, observed_user.to_string()
        )

        # TODO: Inform the remote that we've dropped the presence list.


def should_notify(old_state, new_state):
    if old_state.status_msg != new_state.status_msg:
        return True

    if old_state.state == PresenceState.ONLINE:
        if new_state.state != PresenceState.ONLINE:
            # Always notify for online -> anything
            return True

    if new_state.last_active - old_state.last_active > LAST_ACTIVE_GRANULARITY:
        # Always notify for a transition where last active gets bumped.
        return True

    if old_state.state != new_state.state:
        # Nothing to report.
        return True

    return False


def _format_user_presence_state(state, now):
    content = {
        "presence": state.state,
        "user_id": state.user_id,
    }
    if state.last_active:
        content["last_active_ago"] = now - state.last_active
    if state.status_msg:
        content["status_msg"] = state.status_msg

    return content


class PresenceEventSource(object):
    def __init__(self, hs):
        self.hs = hs
        self.clock = hs.get_clock()
        self.store = hs.get_datastore()

    @defer.inlineCallbacks
    @log_function
    def get_new_events(self, user, from_key, room_ids=None, include_offline=True,
                       **kwargs):
        user_id = user.to_string()
        if from_key is not None:
            from_key = int(from_key)
        room_ids = room_ids or []

        presence = self.hs.get_handlers().presence_handler

        if not room_ids:
            rooms = yield self.store.get_rooms_for_user(user_id)
            room_ids = set(e.room_id for e in rooms)

        user_ids_to_check = set()
        for room_id in room_ids:
            users = yield self.store.get_users_in_room(room_id)
            user_ids_to_check.update(users)

        plist = yield self.store.get_presence_list_accepted(user.localpart)
        user_ids_to_check.update([row["observed_user_id"] for row in plist])

        # Always include yourself. Only really matters for when the user is
        # not in any rooms, but still.
        user_ids_to_check.add(user_id)

        logger.info("[%s] get_new_events checking: %r", user_id, user_ids_to_check)

        max_token = self.store.get_current_presence_token()

        if from_key:
            user_ids_changed = self.store.presence_stream_cache.get_entities_changed(
                user_ids_to_check, from_key,
            )
        else:
            user_ids_changed = user_ids_to_check

        logger.info("[%s] get_new_events changed: %r", user_id, user_ids_changed)

        updates = []
        missing = []
        for user_id in user_ids_changed:
            state = presence.user_to_current_state.get(user_id, None)
            if state is None:
                missing.append(user_id)
            else:
                updates.append(state)

        missing_updates = yield self.store.get_presence_for_users(missing)
        updates.extend(missing_updates)

        now = self.clock.time_msec()

        defer.returnValue(([
            {
                "type": "m.presence",
                "content": _format_user_presence_state(s, now),
            }
            for s in updates
            if include_offline or s.state != PresenceState.OFFLINE
        ], max_token))

    def get_current_key(self):
        logger.info("Fetching presence token: %r", self.store.get_current_presence_token())
        return self.store.get_current_presence_token()

    def get_pagination_rows(self, user, pagination_config, key):
        return self.get_new_events(user, from_key=None, include_offline=False)
