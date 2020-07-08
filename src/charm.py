#! /usr/bin/env python3
from ops.charm import CharmBase

from ops.main import main


import logging
#!/usr/bin/env python3
from ops.model import (
    ActiveStatus,
    BlockedStatus,
)

from slurm_ops_manager import SlurmOpsManager

from interface_slurm_cluster import SlurmClusterRequiresRelation


class SlurmdCharm(CharmBase):

    def __init__(self, *args):
        super().__init__(*args)

        self.slurm_ops_manager = SlurmOpsManager(self, 'slurmd')

        self.slurm_cluster = SlurmClusterRequiresRelation(self, "slurm-cluster")

        self.config = self.model.config
   
        self.framework.observe(self.on.install, self._on_install)
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
