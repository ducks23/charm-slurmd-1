#! /usr/bin/env python3
import logging


from ops.charm import CharmBase

from ops.main import main

from ops.model import (
    ActiveStatus,
    BlockedStatus,
)

from ops.framework import (
    EventBase,
    EventSource,
    Object,
    ObjectEvents,
    StoredState,
)


from slurm_ops_manager import SlurmOpsManager


logger = logging.getLogger()


class ConfigAvailableEvent(EventBase):
    """Slurm Available Event"""


class SlurmdProvidesEvents(ObjectEvents):
    """Slurm Provides Events"""
    config_available = EventSource(ConfigAvailableEvent)


class SlurmdProvidesRelation(Object):

    on = SlurmdProvidesEvents()
    
    _state = StoredState()

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self.charm = charm
        self._relation_name = relation_name

        self._state.set_default(slurm_config=dict())

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

    def _on_relation_created(self, event):
        logger.debug("################ LOGGING RELATION CREATED ####################")

        if self.charm.slurm_ops_manager.slurm_installed:
            event.relation.data[self.model.unit]['hostname'] = \
                self.charm.slurm_ops_manager.hostname
            event.relation.data[self.model.unit]['inventory'] = \
                self.charm.slurm_ops_manager.inventory
            event.relation.data[self.model.unit]['partition'] = \
                self.charm.config['partition']
            event.relation.data[self.model.unit]['default'] = \
                str(self.charm.config['default']).lower()
        else:
            # If we hit this hook/handler before slurm is installed, defer.
            logger.debug("SLURM NOT INSTALLED DEFERING SETTING RELATION DATA")
            event.defer()
            return

    def _on_relation_joined(self, event):
        logger.debug("################ LOGGING RELATION JOINED ####################")

    def _on_relation_changed(self, event):
        logger.debug("################ LOGGING RELATION CHANGED ####################")

        #slurm_config = event.relation.data[event.app].get('slurm_config')
        #if slurm_config:
        #    self._state.slurm_config = json.loads(slurm_config)
        #    self.on.config_available.emit()
    
    def _on_relation_departed(self, event):
        logger.debug("################ LOGGING RELATION DEPARTED ####################")

    def _on_relation_broken(self, event):
        logger.debug("################ LOGGING RELATION BROKEN ####################")

    def get_config(self):
        return self._state.slurm_config


class SlurmdCharm(CharmBase):

    def __init__(self, *args):
        super().__init__(*args)

        self.slurm_ops_manager = SlurmOpsManager(self, 'slurmd')

        self.slurmd = SlurmdProvidesRelation(self, "slurmd")

        self.config = self.model.config
   
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.start, self._on_start)

        self.framework.observe(self.slurmd.on.config_available, self._on_config_available)


    def _on_install(self, event):
        self.slurm_ops_manager.prepare_system_for_slurm()
        self.unit.status = ActiveStatus("Slurm Installed")

    def _on_start(self, event):
        pass

    def _on_config_available(self, event):
        if self.slurm_ops_manager.slurm_installed:
            slurm_config = self.slurmd.get_config()
            #self.charm.slurm_ops_manager.render_config_and_restart(slurm_config)
            logger.debug(slurm_config)
            self.unit.status = ActiveStatus("Slurm config available")
        else:
            self.unit.status = BlockedStatus("Blocked slurm not installed yet")
            event.defer()


if __name__ == "__main__":
    main(SlurmdCharm)
