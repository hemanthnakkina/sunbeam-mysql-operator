#!/usr/bin/env python3
# Copyright 2021 Canonical Ltd.
# See LICENSE file for licensing details.

"""MySQLProvider module"""

import json
import logging
import time

from mysqlserver import MySQL
from ops.framework import StoredState
from ops.relation import ProviderBase

logger = logging.getLogger(__name__)


class MySQLProvider(ProviderBase):
    """
    MySQLProvider class
    """

    _stored = StoredState()

    def __init__(self, charm, name: str, service: str, version: str):
        super().__init__(charm, name, service, version)
        self.charm = charm
        self._stored.set_default(consumers={})
        events = self.charm.on[name]

        self.framework.observe(
            events.relation_joined, self._on_database_relation_joined
        )
        self.framework.observe(
            events.relation_changed, self._on_database_relation_changed
        )
        self.framework.observe(
            events.relation_broken, self._on_database_relation_broken
        )

    ##############################################
    #               RELATIONS                    #
    ##############################################
    def _on_database_relation_joined(self, event):
        self._process_requests(event)

    def _on_database_relation_changed(self, event):
        """Ensure total number of databases requested are available"""
        self._process_requests(event)

    def _process_requests(self, event):
        if not self.charm.unit.is_leader():
            return
        if not self.charm.mysql.is_ready():
            # Mysql may have *just* come up so rather than
            # have to wait for a 5 minute defer just wait
            # a short time before retrying.
            time.sleep(20)
            if not self.charm.mysql.is_ready():
                event.defer()
                return
        data = event.relation.data[event.app]
        rel_id = event.relation.id
        dbs_available = self.charm.mysql.databases()
        logger.debug("SERVER AVAILABLE DB %s", dbs_available)
        dbs = data.get("databases")
        dbs_requested = json.loads(dbs) if dbs else []
        logger.debug("SERVER REQUEST DB %s", dbs_requested)
        if dbs_requested:
            if dbs_available:
                missing = list(set(dbs_requested) - set(dbs_available))
            else:
                missing = dbs_requested
        else:
            logger.debug("No databases requested, nothing to do")
            return
        if missing:
            logger.debug("DBS missing %s", missing)
            for db in missing:
                self.charm.mysql.new_database(db)
        creds = self.credentials(rel_id)
        creds["address"] = self.charm.unit_ip
        username = creds['username']
        logger.debug(creds)
        self.charm.mysql.new_user(creds)
        for db in dbs_requested:
            logger.debug(f"Granting access to {db} to {username}")
            cmd = self.charm.mysql._grant_privileges(creds, db)
            self.charm.mysql._execute_query(cmd)
        event.relation.data[self.charm.app]["databases"] = json.dumps(
            dbs_requested)
        data = {"credentials": dict(creds)}
        event.relation.data[self.charm.app]["data"] = json.dumps(data)

    def _on_database_relation_broken(self, event):
        if not self.charm.unit.is_leader():
            return

        rel_id = event.relation.id
        databases = json.loads(
            event.relation.data[self.charm.app].get("databases")
        )

        if rel_id in self._stored.consumers:
            creds = self.credentials(rel_id)
            self.charm.mysql.drop_user(creds["username"])
            _ = self._stored.consumers.pop(rel_id)

        if self.charm.model.config["autodelete"]:
            self.charm.mysql.drop_databases(databases)

    def is_new_relation(self, rel_id) -> bool:
        if rel_id in self._stored.consumers:
            return False
        else:
            return True

    def credentials(self, rel_id) -> dict:
        """Return MySQL credentials"""
        if self.is_new_relation(rel_id):
            creds = {
                "username": self.new_username(rel_id),
                "password": MySQL.new_password(),
            }
            self._stored.consumers[rel_id] = creds
        else:
            creds = self._stored.consumers[rel_id]
        return creds

    def new_username(self, rel_id) -> str:
        """Return username based in relation id"""
        return f"user_{rel_id}"
