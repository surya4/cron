#### Different scenarios of cron

Do a job after every 10 or x mins. Avoid duplicat by letting only one instance do the job.
#### 1. Here we will use redis Set and remove the events which are over
#### 2. Here we will use redis setNX and set like events at each minute in the hour. Example: 3 = [1234, 345], 45 = [123456, 4567899]