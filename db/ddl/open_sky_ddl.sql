
create table if not exists adsb.open_sky(
        icao24 varchar(50),
        callsign  varchar(50),
        origin_country varchar(50),
        time_position  numeric,
        last_contact numeric,
        longitude , numeric,
        latitude , numeric,
        baro_altitude , numeric,
        on_ground boolean,
        velocity  numeric,
        true_track  numeric,
        vertical_rate  numeric,
        sensors  int,
        geo_altitude , numeric,
        squawk  varchar(50),
        spi boolean,
        position_source int

);

