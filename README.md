# flightclub

An Arrow Flight SQL client.


## Usage:

```console
$ export FLIGHT_CLUB_TOKEN="<redacted>"
$ flightclub --url=https://my-influxdb-database.my-company.com query --db=test 'select * from air_temperature'
    location   | sea_level_degrees | state | tenk_feet_feet_degrees |        time          
---------------+-------------------+-------+------------------------+----------------------
  coyote_creek |              77.2 | CA    |                   40.8 | 2019-09-17 21:36:00  
  santa_monica |              77.3 | CA    |                     40 | 2019-09-17 21:36:00  
  puget_sound  |              77.5 | WA    |                   41.1 | 2019-09-17 21:36:00  
  coyote_creek |              77.1 | CA    |                     41 | 2020-09-22 06:29:20  
  santa_monica |              77.6 | CA    |                   40.9 | 2020-09-22 06:29:20  
  puget_sound  |                78 | WA    |                   40.9 | 2020-09-22 06:29:20  

Warmup: 947.080625ms, Execute: 146.55475ms, DoGet: 172.576625ms, Total: 1.266212s
```


## Install

Homebrew:

```bash
brew install mkmik/flightclub/flightclub
```

Sources:

```bash
go install mkm.pub/flightclub@latest
```
