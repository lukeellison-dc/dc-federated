"""
The worker manager for the DCFServer class.
"""
import os
import hashlib
import time
import json

from dc_federated.backend._constants import INVALID_WORKER, WORKER_ID_KEY, \
    REGISTRATION_STATUS_KEY, PUBLIC_KEY_STR
from dc_federated.backend.backend_utils import message_seriously_wrong
from nacl.encoding import HexEncoder
from nacl.exceptions import BadSignatureError
from nacl.signing import VerifyKey

from tinydb import TinyDB, Query

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)


class WorkerManager(object):
    """
    Manages workers. It maintains a list of allowed workers and registered workers
    and provides an interface of adding, removing, registering and authenticating them.

    Parameters
    ----------

    server_mode_safe: bool
        Whether or not the server should be in safe of unsafe mode. Safe
        mode does not allow unauthenticated workers, with an optional initial
        set of public keys passed via the key_list_parameters. Raises an
        exception if server started in unsafe mode and key_list_file is not
        None.

    key_list_file: str
        The name of the file containing the public keys for valid workers.
        The file is a just list of the public keys, each generated by the
        worker_key_pair_tool tool. All workers are accepted if no workers
        are provided.

    load_last_session_workers: bool
        When running in safe mode, whether or not to load the workers from the
        previous session.

    path_to_keys_db: str
        Path to the database of workers.
    """
    def __init__(self,
                 server_mode_safe,
                 key_list_file,
                 load_last_session_workers=True,
                 path_to_keys_db='.keys_db.json'):
        self.public_keys = {}
        self.allowed_workers = []
        self.registered_workers = {}
        self.public_keys_db = None

        if not server_mode_safe:
            if key_list_file is not None:
                error_str = "Server started in unsafe mode but list of public keys provided. "\
                            "Either explicitly start server in safe mode or do not " \
                            "supply a public key list."
                logger.error(error_str)
                raise ValueError(error_str)
            else:
                logger.info("Server started is running in **** UNSAFE MODE ****")
                self.do_public_key_auth = False
                return

        logger.info("Server started is running in **** SAFE MODE **** - workers will need to use "
                    "public key authentication.")
        self.do_public_key_auth = True
        self.public_keys_db = None

        keys_to_load = []
        if load_last_session_workers:
            keys_to_load = self.init_db(path_to_keys_db)

        if key_list_file is not None:
            with open(key_list_file, 'r') as f:
                keys = f.read().splitlines()
                keys_to_load.extend(keys)

        for key in set(keys_to_load):
            _, success = self.add_worker(key)
            if not success:
                logger.warning(f"Invalid public key {key} - worker not added.")

    def init_db(self, path_to_keys_db):
        """
        Initialize the database of public_keys and return the list of keys.

        Parameters
        ----------

        path_to_keys_db: str
            The location of the database of workers.

        Returns
        -------

        list:
            The list of keys found
        """
        if not os.path.exists(path_to_keys_db):
            logger.warning(f"Unable to locate workers database at {path_to_keys_db} - "
                           f"creating new database.")
        else:
            logger.info("Creating a backup keys database...")
            with open(path_to_keys_db, 'r') as f:
                data = json.load(f)
            with open(path_to_keys_db + '.bak', 'w') as f:
                json.dump(data, f)

            logger.info(f"Backup written to {path_to_keys_db + '.bak'}.")

        self.public_keys_db = TinyDB(path_to_keys_db)
        docs = self.public_keys_db.all()
        keys_to_load = [doc[PUBLIC_KEY_STR] for doc in docs]

        # purge db because all the keys will be added later.
        self.public_keys_db.remove(doc_ids=[doc.doc_id for doc in docs])

        return keys_to_load

    def authenticate_and_add_worker(self, public_key_str, signed_phrase):
        """
        Authenticates the worker and then adds it. Assumes that the
        public key for the required (if required) was added previously.

        Parameters
        ----------
        public_key_str: str
            The public key

        signed_phrase: binary string
            Signed binary string.

        Returns
        -------

        str, bool:
            The worker id and whether or not the operation was successful.
        """
        if self.authenticate_worker(public_key_str, signed_phrase) :
            return self._add_worker(public_key_str)
        else:
            return INVALID_WORKER, False

    def add_worker(self, public_key_str):
        """
        Adds the worker with the given public key to the list of allowed workers.
        Adds the public key of the worker to the set of public keys if necessary.

        Parameters
        ----------

        public_key_str: str
            The public key

        Returns
        -------

        str, bool:
            The worker id and whether or not the operation was successful.
        """
        if self.do_public_key_auth:
            if not self.add_public_key(public_key_str):
                logger.warning(f"Invalid public key {public_key_str} - worker not added")
                return INVALID_WORKER, False
        return self._add_worker(public_key_str)

    def _add_worker(self, public_key_str):
        """
        Internal function for adding worker to the list of allowed workers.

        Parameters
        ----------

        public_key_str: str
            The public key

        Returns
        -------

        str, bool:
            The worker id and whether or not the operation was successful.
        """
        worker_id = self.generate_id_for_worker(public_key_str)
        if self.do_public_key_auth and public_key_str not in self.public_keys:
            err = message_seriously_wrong("trying to add worker without adding worker first")
            logger.error(err)
            return err, False
        if worker_id not in self.allowed_workers:
            self.allowed_workers.append(worker_id)
            self.registered_workers[worker_id] = False
            if self.public_keys_db is not None:
                self.public_keys_db.insert({PUBLIC_KEY_STR: public_key_str})
            logger.info(
                f"Successfully added worker with public key {public_key_str}")
            return worker_id, True
        else:
            logger.info(f"Worker with public key {public_key_str} was added previously "
                        "- no additional actions taken.")
            return worker_id, False

    def set_registration_status(self, worker_id, should_register):
        """
        Sets the registration status of the given worker to the given value.

        Parameters
        ----------

        worker_id: str
            The id of the worker.

        should_register: bool
            Whether the worker_id should be registered or not.

        Returns
        -------

        str:
            The worker id if operation was successful and INVALID_WORKER otherwise.
        """
        if worker_id in self.allowed_workers:
            old_status = self.registered_workers[worker_id]
            self.registered_workers[worker_id] = should_register
            logger.info(f"Set registration status of worker {worker_id} from {old_status} to {should_register}.")
            return worker_id
        else:
            logger.warning(
                f"Please add worker with public key {worker_id} before trying to change registration status.")
            return INVALID_WORKER

    def remove_worker(self, worker_id):
        """
        Removes the worker from the set of allowed workers.

        Parameters
        ----------

        worker_id: str
            The id of the worker to remove.

        Returns
        -------

        str:
            The worker id if operation was successful and INVALID_WORKER otherwise.
        """
        if worker_id in self.allowed_workers:
            self.allowed_workers.remove(worker_id)
            self.delete_public_key(worker_id)
            if self.public_keys_db is not None:
                doc_ids = [doc.doc_id
                           for doc in self.public_keys_db.search(Query()[PUBLIC_KEY_STR] == worker_id)]
                if len(doc_ids) == 0:
                    logger.error(f"Worker {worker_id} not found in workers_db!!!")
                self.public_keys_db.remove(doc_ids=doc_ids)

            logger.info(f"Worker {worker_id} was removed - this worker will "
                        f"no longer be allowed to register or participate in federated learning. ")
            return worker_id
        else:
            logger.warning(f"Attempt to remove non-existent worker {worker_id}.")
            return INVALID_WORKER

    def add_public_key(self, public_key_str):
        """
        Checks that the supplied public key is a valid public key, and
        then adds it to the dictionary of public keys.

        Parameters
        ----------

        public_key_str: str
            UFT-8 encoded version of the public key

        Returns
        -------

        bool:
            True if the public key was added or if it was already in the list of public keys.
            False otherwise.
        """
        if not self.do_public_key_auth:
            return True
        try:
            if public_key_str not in self.public_keys:
                self.public_keys[public_key_str] = VerifyKey(public_key_str.encode(), encoder=HexEncoder)
                return True
            else:
                logger.warning(f"Attempt to add previously added public key {public_key_str}.")
                return True
        except Exception as e:
            logger.warning(e)
            return False

    def delete_public_key(self, public_key_str):
        """
        Removes the public key from the internal dictionary of public keys.

        Parameters
        ----------

        public_key_str: str
            UFT-8 encoded version of the public key

        Returns
        -------

        bool:
            True if the public key was removed or if it was not in the list to begin with.
            False otherwise. 
        """
        if not self.do_public_key_auth:
            return True
        try:
            if public_key_str in self.public_keys:
                del self.public_keys[public_key_str]
                return True
            else:
                logger.warning(f"Attempt to remove unknown public key {public_key_str}.")
                return True
        except Exception as e:
            logger.warning(e)
            return False

    def get_keys(self):
        """
        Returns the list of keys that this authenticator has.

        Returns
        --------

        list of str:
            The set of public keys for the clients.
        """
        return list(self.public_keys.keys())

    def generate_id_for_worker(self, public_key_str):
        """
        Returns the internal id for the worker with public key =
        public_key_str. If mode is not authenticate, this will generate a new
        id for every call with the same public_key_str argument. Otherwise
        it will just return public_key_str.

        Parameters
        ----------

        public_key_str: str
            UFT-8 encoded version of the public key

        Returns
        -------

        str:
            The worker id.
        """
        if self.do_public_key_auth:
            return public_key_str
        else:
            return hashlib.sha224(str(time.time()).encode(
                    'utf-8')).hexdigest() + '_unauthenticated'

    def authenticate_worker(self, public_key_str, signed_message):
        """
        Authenticates a worker with the given public key against the
        given signed message.

        Parameters
        ----------

        public_key_str: str
            UFT-8 encoded version of the public key

        signed_message: str
            UTF-8 encoded signed message

        Returns
        -------

        bool, str:
            The bool is True if the public key matches the singed message, False otherwise.
            The str indicates whether the public key authentication was performed.
        """
        if not self.do_public_key_auth:
            logger.warning("Accepting worker as valid without authentication.")
            logger.warning(
                "Server was likely started without a list of valid public keys from workers.")
            return True
        try:
            if public_key_str not in self.public_keys:
                return False
            self.public_keys[public_key_str].verify(
                signed_message.encode(), encoder=HexEncoder)
        except BadSignatureError:
            logger.warning(
                f"Failed to authenticate worker with public key: {public_key_str}.")
            return False
        else:
            logger.info(
                f"Successfully authenticated worker with public key: {public_key_str}.")
            return True

    def get_worker_list(self):
        """
        Returns the list of workers and their registration status.

        Returns
        -------

        list of dict:
            Each dictionary has keys WORKER_ID_KEY, REGISTRATION_STATUS_KEY giving the
            values.
        """
        return [{WORKER_ID_KEY: worker_id, REGISTRATION_STATUS_KEY: value}
                for worker_id, value in self.registered_workers.items()]

    def is_worker_allowed(self, worker_id):
        """
        Whether or not the worker is allowed or not.

        Returns
        -------

        bool:
            True if worker is allowed False otherwise.
        """
        return worker_id in self.allowed_workers

    def is_worker_registered(self, worker_id):
        """
        Whether or not the worker is allowed or not.

        Returns
        -------

        bool:
            True if worker is allowed False otherwise.
        """
        return worker_id in self.registered_workers and self.registered_workers[worker_id]
