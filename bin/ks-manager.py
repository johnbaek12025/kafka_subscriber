# Copyright 2021 ThinkPool, all rights reserved
"""
"""

__appname__ = "kafka-subscriber"
__version__ = "1.0"


import optparse
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import ksm
from ksm.ks_manager import KSManager


if __name__ == "__main__":
    usage = """%prog [options]"""
    parser = optparse.OptionParser(usage=usage, description=__doc__)
    # parser.add_option(
    #     "--deal-date", metavar="DEAL_DATE", dest="deal_date", help="test request"
    # )

    ksm.add_basic_options(parser)
    (options, args) = parser.parse_args()

    config_dict = ksm.read_config_file(options.config_file)

    config_dict["app_name"] = __appname__
    log_dict = config_dict.get("log", {})
    log_file_name = "ksm.log"
    ksm.setup_logging(
        appname=__appname__,
        appvers=__version__,
        filename=log_file_name,
        dirname=options.log_dir,
        debug=options.debug,
        log_dict=log_dict,
        emit_platform_info=True,
    )

    ks_manager = KSManager()
    ks_manager.initialize(config_dict)
    ks_manager.run()
