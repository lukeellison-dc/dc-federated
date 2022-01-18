"""
Defines the core server class for the federated learning.
Abstracts away the lower level server logic from the federated
machine learning logic.
"""

from dc_federated.backend.dcf_server import DCFServer
from dc_federated.backend.zqm_interface import ZQMInterfaceServer

import logging

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)

logger.info('Starting server as a subprocess.')

zqmi = ZQMInterfaceServer()
server_subprocess_args = zqmi.server_args_request_send()

server = DCFServer(
    register_worker_callback=zqmi.register_worker_send,
    unregister_worker_callback=zqmi.unregister_worker_send,
    return_global_model_callback=zqmi.return_global_model_send,
    is_global_model_most_recent=zqmi.is_global_model_most_recent_send,
    receive_worker_update_callback=zqmi.receive_worker_update_send,
    **server_subprocess_args,
)
server.start_server()
