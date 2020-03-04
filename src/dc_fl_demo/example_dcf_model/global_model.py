import io
from datetime import datetime

import torch

from dc_fl_demo.example_dcf_model.torch_nn_class import ExampleModelClass
from dc_fl_demo.dc_fed_sw import DCFServer


class ExampleGlobalModel(object):
    """
    This is a simple class that illustrates how the DCFServer class may be used to
    implement a federated global model

    """
    def __init__(self):
        self.worker_updates = {}
        self.global_model = ExampleModelClass()
        with open("egm_global_model.torch", 'wb') as f:
            torch.save(self.global_model, f)

        self.global_model_status = str(datetime(2018, 10, 10))

        self.server = DCFServer(
            self.register_worker,
            self.return_global_model,
            self.return_global_model_status,
            self.receive_worker_update
        )

    def register_worker(self, worker_id):
        """
        Register the given worker_id by initializing its update to None.

        Parameters
        ----------

        worker_id: int
            The id of the new worker.
        """
        print(f"Example Global Model: Registering worker {worker_id}")
        self.worker_updates[worker_id] = None

    def return_global_model(self):
        """
        Serializes the current global torch model and sends it back to the worker.

        Returns
        ----------

        byte-stream:
            The current global torch model.
        """
        print(f"Example Global Model: returning global model")
        model_data = io.BytesIO()
        torch.save(self.global_model, model_data)
        return model_data.getvalue()

    def return_global_model_status(self):
        """
        Returns a default model update time of 2018/10/10.

        Returns
        ----------

        str:
            String format of the last model update time.
        """
        print(f"Example Global Model: returning global model status")
        return self.global_model_status

    def receive_worker_update(self, worker_id, model_update):
        """
        Given an update for a worker, adds the the update to the list of updates.

        Returns
        ----------

        str:
            String format of the last model update time.
        """
        if worker_id in self.worker_updates:
            self.worker_updates[worker_id] = \
                torch.load(io.BytesIO(model_update))
            print(f"Model update received from worker {worker_id}")
            print(self.worker_updates[worker_id])
            with open(f"egm_worker_update_{worker_id}.torch", 'wb') as f:
                torch.save(self.worker_updates[worker_id], f)
        else:
            raise ValueError("Unregistered Worker tried to send and update!!")

    def start(self):
        self.server.start_server()
