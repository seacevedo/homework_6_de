package org.example.data;

import java.time.LocalDateTime;

public class Ride {

    public Ride(LocalDateTime pu_datetime, LocalDateTime do_datetime, String pu_location_id, String do_location_id) {
        pickup_datetime = pu_datetime;
        dropoff_datetime = do_datetime;
        PULocationID = pu_location_id;
        DOLocationID = do_location_id;
    }

    public Ride() {
    }

    public LocalDateTime pickup_datetime;
    public LocalDateTime dropoff_datetime;
    public String PULocationID;
    public String DOLocationID;
}
