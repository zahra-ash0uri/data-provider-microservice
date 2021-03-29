class AppVO:

    class ErrorMessages:
        service_unavailable = "Service Unavailable!"
        bad_input = "Bad input format!"
        invalid_input = "Invalid ad_id in input list, Check and try again!"
        wait = "Too Many Requests. Please Wait!"
        unknown_issue = "Unknown issue"

    class RequestBody:
        add_ids = "adIdList"
        from_ = "from"
        to_ = "to"
        standard_datetime_format = "%Y-%m-%d %H:%M:%S"

    class ResponseBody:
        data = "data"
        message = "message"

        total_received_requests = "total_received_requests"
        average_response_time = "average_response_time"
        response_time_99percentile = "response_time_99percentile"

    class MongodbCollectionFields:
        ad_id = "ad_id"
        ad_ctr = "ad_ctr"

        received_at = "received_at"
        response_time = "response_time"
