#! /usr/bin/env python3
from ops.charm import CharmBase

from ops.main import main


import logging
import socket
import json

from ops.framework import (
    EventBase,
    EventSource,
    Object,
    ObjectEvents,
    StoredState,
)

from slurm_ops_manager import SlurmConfig, SlurmOpsManager


logger = logging.getLogger()



class SlurmClusterRequiresRelationEvents(ObjectEvents):
    """SlurmCluster Relation Events"""


class SlurmClusterRequiresRelation(Object):

    on = SlurmClusterRequiresRelationEvents()
    
    _state = StoredState()

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self.charm = charm
        self._relation_name = relation_name

        self.state.set_default(slurm_config_acquired=False)

        self.framework.observe(
            self.charm.on[self._relation_name].relation_created,
            self._on_relation_created
        )

        self.framework.observe(
            self.charm.on[self._relation_name].relation_joined,
            self._on_relation_joined
        )

        self.framework.observe(
            self.charm.on[self._relation_name].relation_changed,
            self._on_relation_changed
        )

        self.framework.observe(
            self.charm.on[self._relation_name].relation_departed,
            self._on_relation_departed
        )

        self.framework.observe(
            self.charm.on[self._relation_name].relation_broken,
            self._on_relation_broken
        )

    @property
    def slurm_config_acquired(self):
        return self._state.slurm_config_acquired

    def _on_relation_created(self, event):
        logger.debug("################ LOGGING RELATION CREATED ####################")

        # 1) Ensure that we have data to access from the charm state object.
        #    we know that if slurm is installed that the slurmd command will be
        #    available. 
        # 2) Use data from the main charm state to fulfil sending the relation data.
        if self.charm.state.slurm_installed:
            event.relation.data[self.model.unit]['hostname'] = \
                self.charm.hostname
            event.relation.data[self.model.unit]['inventory'] = \
                self.charm.slurm_ops_manager.inventory
            event.relation.data[self.model.unit]['partition'] = \
                self.charm.config['partition']
            event.relation.data[self.model.unit]['default'] = \
                self.charm.config['default']
        else:
            # If we hit this hook before slurm is installed, defer.
            logger.debug("SLURM NOT INSTALLED DEFERING SETTING RELATION DATA")
            event.defer()
            return

    def _on_relation_joined(self, event):
        logger.debug("################ LOGGING RELATION JOINED ####################")

    def _on_relation_changed(self, event):
        logger.debug("################ LOGGING RELATION CHANGED ####################")

        slurm_config = event.relation.data[event.app].get('slurm_config')
        self.charm.slurm_ops_manager.on.render_config_and_restart.emit(
            SlurmConfig(slurm_config)
        )
        self._state.slurm_config_acquired = True
    
    def _on_relation_departed(self, event):
        logger.debug("################ LOGGING RELATION DEPARTED ####################")

    def _on_relation_broken(self, event):
        logger.debug("################ LOGGING RELATION BROKEN ####################")


class SlurmdCharm(CharmBase):

    state = StoredState()

    def __init__(self, *args):
        super().__init__(*args)

        self.config = self.model.config

        self.slurm_ops_manager = SlurmOpsManager(self, 'slurmd')
        self.slurm_cluster = SlurmClusterRequiresRelation(self, "slurm-cluster")
        
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.start, self._on_start)

        self.framework.observe(self.on.start, self._on_start)


    def _on_install(self, event):
        self.slurm_ops_manager.prepare_system_for_slurm()

    def _on_start(self, event):
        if self.slurm_cluster.slurm_config_acquired and self.slurm_ops_manager.slurm_installed:
            self.unit.status = ActiveStatus("Slurmd Available")
        else:
            if not self.slurm_cluster.slurm_config_acquired:
                self.unit.status = BlockedStatus("Need relation to slurm controller.")
            elif not self.slurm_ops_manager.slurm_installed:
                self.unit.status = WaitingStatus("Waiting on slurm install to complete...")
            event.defer()
            return 
    


if __name__ == "__main__":
    main(SlurmdCharm)
