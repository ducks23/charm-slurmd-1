#! /usr/bin/env python3
"""libraries needed for charm."""
from interface_slurmd import SlurmdProvides
from ops.charm import CharmBase
from ops.main import main
from ops.model import (
    ActiveStatus,
    BlockedStatus,
)
from slurm_ops_manager import SlurmOpsManager
from stdlib import (
    json,
    logging,
)

logger = logging.getLogger()


class SlurmdCharm(CharmBase):
    """representing the comtpute node portion of Slurm Workload Manager."""

    def __init__(self, *args):
        """Initialize charm and configure states and events to observe.

        Component: SlurmOpsManager class that installs the slurm binary.
        Interface: SlurmdProvides which is responsible for communicating
        with slurmctld.
        """
        super().__init__(*args)
        self.slurm_ops_manager = SlurmOpsManager(self, 'slurmd')
        self.slurmd = SlurmdProvides(self, "slurmd")

        self.config = self.model.config
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.start, self._on_config_available)

        self.framework.observe(self.slurmd.on.config_available, self._on_config_available)

    def _on_install(self, event):
        self.slurm_ops_manager.prepare_system_for_slurm()
        self.unit.status = ActiveStatus("Slurm Installed")

    def _on_config_available(self, event):
        if (self.slurm_ops_manager.slurm_installed and self.slurmd.config_available):

            try:
                slurm_config = json.loads(self.slurmd.get_slurm_config())
            except json.JSONDecodeError as e:
                self.unit.status = BlockedStatus("Error decoding JSON, please debug.")
                logger.debug(e)
                return

            logger.debug(slurm_config)
            self.slurm_ops_manager.render_config_and_restart(slurm_config)
            self.unit.status = ActiveStatus("Slurmd Available")

        else:
            self.unit.status = BlockedStatus("Blocked need relation to slurmctld.")
            event.defer()


if __name__ == "__main__":
    main(SlurmdCharm)
