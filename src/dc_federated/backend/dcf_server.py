"""
Defines the core server class for the federated learning.
Abstracts away the lower level server logic from the federated
machine learning logic.
"""
import gevent
from gevent import monkey; monkey.patch_all()
from gevent import Greenlet, queue, pool

import os
import json
import os.path
import zlib
import msgpack
import hashlib

from bottle import Bottle, run, request, response, auth_basic, ServerAdapter

from dc_federated.backend._constants import *
from dc_federated.backend.backend_utils import *
from dc_federated.utils import get_host_ip
from dc_federated.backend.backend_utils import is_valid_model_dict
from dc_federated.backend._worker_manager import WorkerManager

import logging

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)


class DCFServer(object):
    """
    This class abstracts away the lower level communication logic for
    the central server/node from the actual federated learning logic.
    It interacts with the central server node via the 4 callback functions
    passed in the constructor. For an example usage please refer to the
    package dc_federated.example_dcf+model.

    Parameters
    ----------

    register_worker_callback:
        This function is expected to take the id of a newly registered
        worker and should contain the application specific logic for
        dealing with a new worker joining the federated learning pool.

    unregister_worker_callback:
        This function is expected to take the id of a newly unregistered
        worker and should contain the application specific logic for
        dealing with a worker leaving the federated learning pool.

    return_global_model_callback: () -> dict
        This function is expected to return a dictionary with the
        GLOBAL_MODEL: containing the serialization of the global model
        GLOBAL_MODEL_VERSION: containing the global model itself.

    is_global_model_most_recent:  str -> bool
        Returns the True if the model version given in the string is the
        most recent one - otherwise returns False.

    receive_worker_update_callback: dict -> bool
        This function should receive a worker-id and an application
        dependent binary serialized update from the worker. The
        server code ensures that the worker-id was previously
        registered.

    server_mode_safe: bool
        Whether or not the server should be in safe of unsafe mode. Safe
        mode does not allow unauthenticated workers with the optional initial
        set of public keys passed via the key_list_parameters. Raises
        an exception if server started in unsafe mode and key_list_file
        is not None.

    key_list_file: str
        The name of the file containing the public keys for valid workers.
        The public keys are given one key per line, with each key being
        generated by the worker_key_pair_tool.py tool. If None, then
        no authentication is performed.

    load_last_session_workers: bool (default True)
        When running in safe mode, whether or not to load the workers
        from the previous session.

    path_to_keys_db: str
        Path to the database of workers' public keys that has been added.

    server_host_ip: str (default None)
        The ip-address of the host of the server. If None, then it
        uses the ip-address of the current machine.

    server_port: int (default 8080)
        The port at which the serer should listen to. If None, then it
        uses the port 8080.

    ssl_enabled: bool (default False)
        Enable SSL/TLS for server/workers communications.

    ssl_keyfile: str
        Must be a valid path to the key file.
        This is mandatory if ssl_enabled, ignored otherwise.

    ssl_certfile: str
        Must be a valid path to the certificate.
        This is mandatory if ssl_enabled, ignored otherwise.

    model_check_interval: int
        The interval of time between the server checking for an updated
        model for the long polling.
    """
    def __init__(
        self,
        register_worker_callback,
        unregister_worker_callback,
        return_global_model_callback,
        is_global_model_most_recent,
        receive_worker_update_callback,
        server_mode_safe,
        key_list_file,
        load_last_session_workers=True,
        path_to_keys_db='.keys_db.json',
        server_host_ip=None,
        server_port=8080,
        ssl_enabled=False,
        ssl_keyfile=None,
        ssl_certfile=None,
        model_check_interval=10,
        debug=False
    ):
        self.server_host_ip = get_host_ip() if server_host_ip is None else server_host_ip
        self.server_port = server_port

        self.register_worker_callback = register_worker_callback
        self.unregister_worker_callback = unregister_worker_callback
        self.return_global_model_callback = return_global_model_callback
        self.is_global_model_most_recent = is_global_model_most_recent
        self.receive_worker_update_callback = receive_worker_update_callback

        self.worker_manager = WorkerManager(server_mode_safe,
                                            key_list_file,
                                            load_last_session_workers,
                                            path_to_keys_db)

        self.gevent_pool = pool.Pool(None)
        self.model_version_req_dict = {}
        self.model_check_interval = model_check_interval
        self.debug = debug

        self.ssl_enabled = ssl_enabled

        if ssl_enabled:
            if ssl_certfile is None or ssl_keyfile is None:
                raise RuntimeError(
                    "When ssl is enabled, both a certfile and keyfile must be provided")
            if not os.path.isfile(ssl_certfile):
                raise IOError(
                    "The provided SSL certificate file doesn't exist")
            if not os.path.isfile(ssl_keyfile):
                raise IOError("The provided SSL key file doesn't exist")
            self.ssl_keyfile = ssl_keyfile
            self.ssl_certfile = ssl_certfile

    @staticmethod
    def is_admin(username, password):
        """
        Callback for bottle to check that the requester is authorized to
        act as an admin for the server.

        Parameters
        ----------
        username: str
            The admin username.

        password: str
            The admin password.

        Returns
        -------

        bool:
            True if the user/password us valid, false otherwise.
        """
        adm_username = os.environ.get(ADMIN_USERNAME)
        adm_password = os.environ.get(ADMIN_PASSWORD)

        if adm_username is None or adm_password is None:
            return False

        return username == adm_username and password == adm_password

    @staticmethod
    def validate_input(dct, keys, data_types):
        """
        Validates the given input dictionary dct by ensuring that all the
        keys are in dct and they have the corresponding types.

        Parameters
        ----------

        dct: object
            The object to verify as a dictionary

        keys: str list
            Lisf of keys to test for.

        data_types:
            The types of the elements in the keys.

        Returns
        -------

        dict:
            The keys for which the checks failed - otherwise
        """
        inp_invalid = verify_dict(dct, keys, data_types)
        if len(inp_invalid) > 0:
            error_str = "Invalid input: failed to get the following keys from JSON input: " \
                        f"{inp_invalid}"
            return {
                ERROR_MESSAGE_KEY: error_str
            }
        else:
            return {}

    def add_and_register_worker(self):
        """
        Registers the worker, adding it to the list of allowed workers
        if necessary.

        Returns
        -------

        str:
            The id of the new client, or INVALID_WORKER if the process failed.
        """
        worker_data = request.json
        valid_failed = DCFServer.validate_input(worker_data, [PUBLIC_KEY_STR], [str])
        if ERROR_MESSAGE_KEY in valid_failed:
            logger.error(valid_failed[ERROR_MESSAGE_KEY])
            return valid_failed[ERROR_MESSAGE_KEY]

        signed_phrase = "" if SIGNED_PHRASE not in worker_data else worker_data[SIGNED_PHRASE]
        worker_id, success = \
            self.worker_manager.authenticate_and_add_worker(worker_data[PUBLIC_KEY_STR],
                                                            signed_phrase)
        if worker_id == INVALID_WORKER:
            return worker_id

        if not self.worker_manager.is_worker_registered(worker_id):
            self.worker_manager.set_registration_status(worker_id, True)
            self.register_worker_callback(worker_id)

        return worker_id

    def admin_list_workers(self):
        """
        List all registered workers

        Returns
        -------

        str:
            JSON in string form containing id of workers and their registration status.
        """
        return json.dumps(self.worker_manager.get_worker_list())

    def admin_add_worker(self):
        """
        Add a new worker to the list or allowed workers via the admin API.

        JSON Body:
            public_key_str: string The public key associated with the worker

        Returns
        -------

        str:
            JSON in string form either containing the id of the worker added + its
            registration status or an error message if that failed.
        """
        worker_data = request.json

        valid_failed = DCFServer.validate_input(worker_data,
                                [PUBLIC_KEY_STR, REGISTRATION_STATUS_KEY],
                                [str, bool])
        if ERROR_MESSAGE_KEY in valid_failed:
            logger.error(valid_failed[ERROR_MESSAGE_KEY])
            return json.dumps(valid_failed)

        logger.info("Admin is adding a new worker...")

        worker_id, success = self.worker_manager.add_worker(worker_data[PUBLIC_KEY_STR])
        if worker_id == INVALID_WORKER:
            err_msg = f"Unable to validate public key (short) {worker_data[PUBLIC_KEY_STR][0:WID_LEN]} "\
                       "- worker not added."
            logger.warning(err_msg)
            return json.dumps({
                ERROR_MESSAGE_KEY: err_msg
            })

        if not success:
            return json.dumps({ERROR_MESSAGE_KEY: f"Worker {worker_id[0:WID_LEN]} already exists."})

        worker_id = self.worker_manager.set_registration_status(
            worker_id, worker_data[REGISTRATION_STATUS_KEY])

        if worker_id == INVALID_WORKER:
            error_str = message_seriously_wrong("worker was just added but now being reported as not added")
            logger.error(error_str)
            return json.dumps({ERROR_MESSAGE_KEY: error_str})

        if worker_data[REGISTRATION_STATUS_KEY]:
            self.register_worker_callback(worker_id)

        return json.dumps({
            SUCCESS_MESSAGE_KEY: f"Successfully added worker {worker_id[0:WID_LEN]}.",
            WORKER_ID_KEY: worker_id,
            REGISTRATION_STATUS_KEY: worker_data[REGISTRATION_STATUS_KEY]
        })

    def admin_delete_worker(self, worker_id):
        """
        Delete a new worker from the list of allowed workers via the admin API.

        Parameters
        ----------

        worker_id: str
            The id of the worker to delete

        Returns
        -------

        str:
            JSON in string form containing either id of worker removed
            or error message if the operation failed for some reason.
        """
        logger.info(f"Admin is removing worker {worker_id[0:WID_LEN]}...")
        was_registered = self.worker_manager.is_worker_registered(worker_id)
        worker_id = self.worker_manager.set_registration_status(worker_id, False)
        if worker_id != INVALID_WORKER:
            if was_registered:
                self.unregister_worker_callback(worker_id)
                logger.info(f"Worker {worker_id[0:WID_LEN]} was unregistered (removal)")

        worker_id = self.worker_manager.remove_worker(worker_id)
        if worker_id == INVALID_WORKER:
            return json.dumps({ERROR_MESSAGE_KEY: f"Attempt to remove unknown worker {worker_id[0:WID_LEN]}."})

        return json.dumps({
            WORKER_ID_KEY: worker_id,
            SUCCESS_MESSAGE_KEY: f"Successfully removed worker {worker_id[0:WID_LEN]}."
        })

    def admin_set_worker_status(self, worker_id):
        """
        Set worker status to (REGISTRATION_STATUS_KEY = True or False) via the admin API.

        Parameters
        ----------

        worker_id: str
            The id of the worker to set the status for.

        Returns
        -------

        str:
            JSON in string form containing either id of worker removed and
            registration status or error message if the operation failed for
            some reason.
        """
        worker_data = request.json

        logger.info(f"Admin is setting the status of {worker_id[0:WID_LEN]}...")
        valid_failed = DCFServer.validate_input(worker_data, [REGISTRATION_STATUS_KEY], [bool])
        if ERROR_MESSAGE_KEY in valid_failed:
            logger.error(valid_failed[ERROR_MESSAGE_KEY])
            return json.dumps(valid_failed)

        was_registered = self.worker_manager.is_worker_registered(worker_id)
        worker_id = self.worker_manager.set_registration_status(
            worker_id, worker_data[REGISTRATION_STATUS_KEY])

        if worker_id == INVALID_WORKER:
            return json.dumps({
                ERROR_MESSAGE_KEY: f"Attempt at changing worker status failed - "
                                   f"please ensure this worker was added: {worker_id[0:WID_LEN]}."
            })

        logger.info(f"New {worker_id[0:WID_LEN]} status is {REGISTRATION_STATUS_KEY}: "
                    f"{worker_data[REGISTRATION_STATUS_KEY]}")

        if not was_registered and worker_data[REGISTRATION_STATUS_KEY]:
            self.register_worker_callback(worker_id)

        if was_registered and not worker_data[REGISTRATION_STATUS_KEY]:
            self.unregister_worker_callback(worker_id)

        return json.dumps({
            SUCCESS_MESSAGE_KEY: f"Successfully changed status for worker {worker_id[0:WID_LEN]}.",
            WORKER_ID_KEY: worker_id,
            REGISTRATION_STATUS_KEY: worker_data[REGISTRATION_STATUS_KEY]
        })

    def receive_worker_update(self, worker_id):
        """
        This receives the update from a worker and calls the corresponding callback function.
        Expects that the worker_id and model-update were sent using the DCFWorker.send_model_update()

        Returns
        -------

        str:
            If the update was successful then "Worker update received"
            Otherwise any exception that was raised.
        """
        try:
            worker_data = request.files
            if SIGNED_PHRASE not in worker_data:
                error_message = f"{SIGNED_PHRASE} not found in worker update payload."
                logger.error(error_message)
                return json.dumps({ERROR_MESSAGE_KEY: error_message})

            model_update = zlib.decompress(worker_data[WORKER_MODEL_UPDATE_KEY].file.read())

            verify_worker = self.worker_manager.authenticate_worker(
                worker_id,
                worker_data[SIGNED_PHRASE].file.read().decode('utf-8'),
                hashlib.sha256(model_update).digest()
            )
            if not verify_worker:
                logger.error(f"Unable to verify worker with id {worker_id[0:WID_LEN]}")
                return INVALID_WORKER

            if not self.worker_manager.is_worker_allowed(worker_id):
                logger.warning(f"Unknown worker {worker_id[0:WID_LEN]} tried to send an update.")
                return INVALID_WORKER

            if not self.worker_manager.is_worker_registered(worker_id):
                logger.warning(f"Unregistered worker {worker_id[0:WID_LEN]} tried to send an update.")
                return UNREGISTERED_WORKER

            logger.info(f'Received model update from worker {worker_id[0:WID_LEN]}.')
            return self.receive_worker_update_callback(worker_id, model_update)

        except Exception as e:
            logger.warning(e)
            return str(e)

    def check_model_version_updated(self, worker_id, body, last_worker_model_version):
        """
        Greenlet function run to check with the implementation of the
        algorithm server-side logic to see if the global model is ready.

        Parameters
        ---------

        body: gevent.queue.Queue
            The Queue used to return the data to the calling worker and
            fulfill the WSGI promise/map.

        last_worker_model_version: object
            The version of the last model that the worker was using.
        """
        while self.is_global_model_most_recent(last_worker_model_version):
            gevent.sleep(self.model_check_interval)

        model_update = self.return_global_model_callback()
        if not is_valid_model_dict(model_update):
            logger.error(f"Expected dictionary with {GLOBAL_MODEL} and {GLOBAL_MODEL_VERSION} keys - "
                         "return_global_model_callback() implementation is incorrect")
        body.put(GLOBAL_MODEL_UPDATED_STRING)
        body.put(StopIteration)
        logger.info(f"Notified global model version changed to {worker_id[0:WID_LEN]}.")

        # clean up the list of model requests for this worker
        if len(self.model_version_req_dict[worker_id]) > 0:
            self.model_version_req_dict[worker_id].pop()
        if len(self.model_version_req_dict[worker_id]) > 0:
            message_seriously_wrong(f"in 'check_model_ready', "
                                    f"more than one entry in the 'mode_req_dict' for {worker_id[0:WID_LEN]}")

    def notify_me_if_gm_version_updated(self):
        """
        Sends a respond back to a worker indicating that the current
        global model is more recent than the model version indicated by
        the worker.
        """
        try:
            query_request = request.json
            valid_failed = DCFServer.validate_input(
                query_request,
                [WORKER_ID_KEY, LAST_WORKER_MODEL_VERSION, SIGNED_PHRASE],
                [str, object, str]
            )
            if ERROR_MESSAGE_KEY in valid_failed:
                logger.error(valid_failed[ERROR_MESSAGE_KEY])
                return json.dumps({ERROR_MESSAGE_KEY: valid_failed[ERROR_MESSAGE_KEY]})

            worker_id = query_request[WORKER_ID_KEY]
            if not self.worker_manager.is_worker_allowed(worker_id):
                logger.warning(f"Unknown worker {worker_id[0:WID_LEN]} tried to get the global model.")
                return INVALID_WORKER

            if not self.worker_manager.verify_challenge(worker_id, query_request[SIGNED_PHRASE]):
                logger.error(f"Failed to verify worker with id {worker_id[0:WID_LEN]}")
                return INVALID_WORKER

            if not self.worker_manager.is_worker_registered(worker_id):
                logger.warning(f"Unregistered worker {worker_id[0:WID_LEN]} tried to get the global model.")
                return UNREGISTERED_WORKER

            logger.info(f"Received request for global model version change notification from {worker_id[0:WID_LEN]}.")
            # in case a new request is made, terminate the old one
            if worker_id in self.model_version_req_dict and \
                    len(self.model_version_req_dict[worker_id]) > 0:
                old_g, old_b = self.model_version_req_dict[worker_id].pop()
                msg = f"New request for global model version change notification received from {worker_id[0:WID_LEN]} - " \
                      "existing request terminated."
                logger.info(msg)
                old_b.put(msg)
                old_b.put(StopIteration)
                old_g.kill()
                if len(self.model_version_req_dict[worker_id]) > 0:
                    message_seriously_wrong(f"in 'return_global_model', "
                                            f"more than one entry in the 'mode_req_dict' for {worker_id[0:WID_LEN]}")
            body = gevent.queue.Queue()
            g = Greenlet(self.check_model_version_updated, worker_id, body, query_request[LAST_WORKER_MODEL_VERSION])
            self.gevent_pool.add(g)
            if worker_id not in self.model_version_req_dict:
                self.model_version_req_dict[worker_id] = []
            self.model_version_req_dict[worker_id].append((g, body))
            g.start()

            return body

        except Exception as e:
            logger.warning(str(e.__class__) + str(e))
            return str(e)

        # If the same worker made a long poll request previously,
        # terminate that request

    def return_global_model(self):
        """
        Returns the global model by using the provided callback using gevent
        based long polling. It spawns a gevent Greenlet (a pseudo-thread) for
        check_model_ready, which returns a model when ready, but otherwise
        waits.

        Returns
        -------

        gevent.queue.Queue or str:
            The Queue object that returns the model in a long polling or
            a string indicating an error has occured.
        """
        try:
            query_request = request.json

            valid_failed = DCFServer.validate_input(
                query_request, [WORKER_ID_KEY, SIGNED_PHRASE], [str, str])
            if ERROR_MESSAGE_KEY in valid_failed:
                logger.error(valid_failed[ERROR_MESSAGE_KEY])
                return json.dumps({ERROR_MESSAGE_KEY: valid_failed[ERROR_MESSAGE_KEY]})

            worker_id = query_request[WORKER_ID_KEY]
            if not self.worker_manager.is_worker_allowed(worker_id):
                logger.warning(f"Unknown worker {worker_id[0:WID_LEN]} tried to get the global model.")
                return INVALID_WORKER

            if not self.worker_manager.verify_challenge(worker_id, query_request[SIGNED_PHRASE]):
                logger.error(f"Failed to verify worker with id {worker_id[0:WID_LEN]}")
                return INVALID_WORKER

            if not self.worker_manager.is_worker_registered(worker_id):
                logger.warning(f"Unregistered worker {worker_id[0:WID_LEN]} tried to get the global model.")
                return UNREGISTERED_WORKER

            logger.info(f"Returned global model to {worker_id[0:WID_LEN]}.")
            return zlib.compress(msgpack.packb(self.return_global_model_callback()))

        except Exception as e:
            logger.warning(str(e.__class__) + str(e))
            return str(e)

    @staticmethod
    def enable_cors():
        """
        Enable the cross origin resource for the server.
        """
        response.add_header('Access-Control-Allow-Origin', '*')
        response.add_header('Access-Control-Allow-Methods',
                            'GET, POST, PUT, OPTIONS')
        response.add_header('Access-Control-Allow-Headers',
                            'Origin, Accept, Content-Type, X-Requested-With, X-CSRF-Token')

    def start_server(self, server_adapter=None):
        """
        Sets up all the routes for the server and starts it.

        server_backend: bottle.ServerAdapter (default None)
            The server adapter to use. The default bottle.WSGIRefServer is used if none is given.
            WARNING: If given, this will over-ride the host-ip and port passed as parameters to this
            object.
        """
        application = Bottle()
        application.route(f"/{REGISTER_WORKER_ROUTE}",
                          method='POST', callback=self.add_and_register_worker)
        application.route(f"/{CHALLENGE_PHRASE_ROUTE}/<worker_id>",
                          method='GET', callback=self.worker_manager.get_challenge_phrase)
        application.route(f"/{RETURN_GLOBAL_MODEL_ROUTE}",
                          method='POST', callback=self.return_global_model)
        application.route(f"/{NOTIFY_ME_IF_GM_VERSION_UPDATED_ROUTE}",
                          method='POST', callback=self.notify_me_if_gm_version_updated)
        application.route(f"/{RECEIVE_WORKER_UPDATE_ROUTE}/<worker_id>",
                          method='POST', callback=self.receive_worker_update)

        application.add_hook('after_request', self.enable_cors)

        # Admin routes
        application.get(
            f"/{WORKERS_ROUTE}", callback=auth_basic(self.is_admin)(self.admin_list_workers))
        application.post(
            f"/{WORKERS_ROUTE}", callback=auth_basic(self.is_admin)(self.admin_add_worker))
        application.delete(f"/{WORKERS_ROUTE}/<worker_id>",
                           callback=auth_basic(self.is_admin)(self.admin_delete_worker))
        application.put(f"/{WORKERS_ROUTE}/<worker_id>",
                        callback=auth_basic(self.is_admin)(self.admin_set_worker_status))

        if server_adapter is not None and isinstance(server_adapter, ServerAdapter):
            self.server_host_ip = server_adapter.host
            self.server_port = server_adapter.port
            run(application, server=server_adapter, debug=self.debug, quiet=True)
        elif self.ssl_enabled:
            run(application,
                host=self.server_host_ip,
                port=self.server_port,
                server='gunicorn',
                worker_class='gevent',
                keyfile=self.ssl_keyfile,
                certfile=self.ssl_certfile,
                debug=self.debug,
                timeout=60*60*24,
                quiet=True)
        else:
            run(application,
                host=self.server_host_ip,
                port=self.server_port,
                server='gunicorn',
                worker_class='gevent',
                debug=self.debug,
                timeout=60*60*24,
                quiet=True)