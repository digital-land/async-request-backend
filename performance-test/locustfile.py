import time

from locust import HttpUser, task


class PlatformServiceUser(HttpUser):

    @task
    def create_request_and_poll_for_result(self):
        create_request = {
            "params": {
                "type": "check_file",
                "collection": "article_4_direction",
                "dataset": "article_4_direction_area",
                "original_filename": "bogdan-farca-CEx86maLUSc-unsplash.jpg",
                "uploaded_filename": "B1E16917-449C-4FC5-96D1-EE4255A79FB1"
            }
        }
        creation_response = self.client.post("/requests", json=create_request)
        request_id = creation_response.json()['id']

        def _wait_for_request_status(
                expected_status, timeout_seconds=30, interval_seconds=1
        ):
            seconds_waited = 0
            actual_status = "UNKNOWN"
            while seconds_waited <= timeout_seconds:
                response = self.client.get(f"/requests/{request_id}", name="/requests/{id}")
                if response.json()['status'] == expected_status:
                    return
                else:
                    time.sleep(interval_seconds)
                    seconds_waited += interval_seconds
                    print(
                        f"Waiting {interval_seconds} second(s) for expected status of "
                        f"{expected_status} on request {request_id}"
                    )
        _wait_for_request_status('COMPLETE')
