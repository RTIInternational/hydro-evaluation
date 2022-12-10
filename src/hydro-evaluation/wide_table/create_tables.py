import psycopg2
import psycopg2.extras
import config


def create_nwm_table():
    """
    0   reference_time    datetime64[ns]
    1   value_time        datetime64[ns]
    2   nwm_feature_id    int64         
    3   value             float32       
    4   usgs_site_code    category      
    5   configuration     category      
    6   measurement_unit  category      
    7   variable_name     category  
    """

    # conn = psycopg2.connect(CONNECTION, cursor_factory=psycopg2.extras.RealDictCursor)
    conn = psycopg2.connect(config.CONNECTION)

    with conn:
        with conn.cursor() as cursor:
            try:
                query = """
                    CREATE TABLE nwm_data (
                        id SERIAL PRIMARY KEY, 
                        reference_time TIMESTAMPTZ NOT NULL,
                        value_time TIMESTAMPTZ NOT NULL,
                        nwm_feature_id INTEGER,    
                        value             float8,
                        configuration     text,     
                        measurement_unit  text,     
                        variable_name     text
                    );
                """
                cursor.execute(query)
                conn.commit()
            except (Exception, psycopg2.Error) as error:
                print(error)
            cursor.close()
        # conn.close()


if __name__ == "__main__":
    create_nwm_table()
