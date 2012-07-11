-record(la_record, {user, timestamp, 'query', query_time, response_time, flags = []} ).

-record(la_stats, {command, min_cost_time, max_cost_time, avg_cost_time, last_execute_timestamp, times, flags = []}).
