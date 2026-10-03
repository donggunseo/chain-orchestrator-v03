"""Source-specific Mock modules; this package is not imported by clinical Workflow."""
from .emr import EMRAdapter
from .nursing import NursingAdapter
from .ocs import OCSAdapter
from .lis import LISAdapter
from .ris_pacs import RISPACSAdapter
from .manual import ManualAdapter
