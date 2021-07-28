from numpy import array
import pandas as pd
import requests
import psycopg2 as pg2
import psycopg2.extras
import sqlalchemy

#https://github.com/dadonnelly316/poster-kata.git

#to do: make sure that changing dates will update the database

#https://swapi.dev/


ENDPOINT = 'https://swapi.dev/api/starships/?page=1'
ENV = 'dev'


#How can the credentials be hashed???
DB_NAME_DW='dw'
DB_USER_DW='dw_writer'
DB_PASS_DW='badpassword2'
DB_HOST_DW='db' #host name resolves to the name of the service that starts the db?
DB_PORT_DW=5432



def connect(name: str, user: str, pw: str, host: str, port: str):
    try:
        conn = pg2.connect(dbname=name, user=user, password=pw, host=host, port=port)
        engine = sqlalchemy.create_engine(f'postgresql+psycopg2://{user}:{pw}@{host}:{port}/{name}')

    except:
        raise ValueError(f'Unable to connect to the {name} database')
    return conn,engine



def init_databases(conn: pg2.connect):
    connect, engine = conn
    curr=connect.cursor(cursor_factory=psycopg2.extras.DictCursor)

    create_table_query = """

        CREATE TABLE IF NOT EXISTS public.source
            (
                poster_content character varying(18) COLLATE pg_catalog."default",
                quantity integer,
                price numeric,
                email character varying(26) COLLATE pg_catalog."default" NOT NULL,
                sales_rep character varying(17) COLLATE pg_catalog."default",
                promo_code character varying(5) COLLATE pg_catalog."default",
                "timestamp" date,
                cust_id integer
            );

        CREATE TABLE IF NOT EXISTS public.dw
            (
                film_name character varying(200) COLLATE pg_catalog."default" NOT NULL,
                date_filmed date,
                poster_content character varying(18) COLLATE pg_catalog."default",
                price numeric,
                sales_rep character varying(17) COLLATE pg_catalog."default",
                promo_code character varying(5) COLLATE pg_catalog."default",
                "timestamp" date,
                cust_id integer,
                source_timestamp date
            )
              
    """

    curr.execute(create_table_query)


    query_populate_source = """

            DO
            $do$
            BEGIN
            IF (SELECT COUNT(*) FROM SOURCE)=0 then
                    INSERT INTO source
                        VALUES
                            ('Millennium Falcon', 7, 2.9, 'sally_skywalker@gmail.com', 'tej@swposters.com', 'radio', '2021-07-25', 1);
            END IF;
            END
            $do$

    """
    curr.execute(query_populate_source)
    connect.commit()
    connect.close()



def source_fetch(conn: pg2.connect):

    connect,engine=conn #remove unused engine variable???

    curr=connect.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query = """
    SELECT DISTINCT 
          POSTER_CONTENT
        , PRICE, SALES_REP
        , PROMO_CODE
        , CUST_ID
        , TIMESTAMP AS SOURCE_TIMESTAMP 
     FROM SOURCE;
     """

    curr.execute(query)
    results= curr.fetchall()
    connect.close()

    return results


def query_dw(conn: list, df: pd.DataFrame):

    connect, engine = conn

    curr=connect.cursor(cursor_factory=psycopg2.extras.DictCursor)

    try:
        df.to_sql('STAGE_FILMS', engine, index=False, if_exists='replace')
    except:
        raise ValueError('Could not write the stage_films table to the database.')
        


    query = """

        INSERT INTO DW (FILM_NAME, DATE_FILMED, POSTER_CONTENT, PRICE, SALES_REP, PROMO_CODE, TIMESTAMP, CUST_ID, SOURCE_TIMESTAMP)
            SELECT FILM_NAME
                 , CAST(RELEASE_DATE AS DATE)
                 , SHIP_NAMES
                 , CAST(PRICE AS DECIMAL)
                 , SALES_REP
                 , PROMO_CODE
                 , CURRENT_DATE
                 , CUST_ID
                 , SOURCE_TIMESTAMP
        FROM "STAGE_FILMS"
        WHERE (FILM_NAME, SHIP_NAMES, CUST_ID) NOT IN (SELECT FILM_NAME, POSTER_CONTENT, CUST_ID FROM DW);

        UPDATE DW
            SET   DATE_FILMED=SOURCE.RELEASE_DATE
                , TIMESTAMP=CURRENT_DATE
            FROM (SELECT DISTINCT CAST(RELEASE_DATE AS DATE) AS RELEASE_DATE ,FILM_NAME FROM "STAGE_FILMS") AS SOURCE
            WHERE DW.DATE_FILMED!=SOURCE.RELEASE_DATE                                         --NULL SHOULDN'T BE CONSIDERED WHEN USING THE != OPERATOR
                AND DW.FILM_NAME=SOURCE.FILM_NAME;

        UPDATE DW
            SET   POSTER_CONTENT=SOURCE.SHIP_NAMES
                , PRICE=SOURCE.PRICE
                , SALES_REP=SOURCE.SALES_REP
                , PROMO_CODE=SOURCE.PROMO_CODE
                , CUST_ID=SOURCE.CUST_ID
                , TIMESTAMP=CURRENT_DATE
                , SOURCE_TIMESTAMP=SOURCE.SOURCE_TIMESTAMP
            FROM (SELECT
                      SHIP_NAMES
                    , CAST(PRICE AS DECIMAL) AS PRICE
                    , SALES_REP
                    , PROMO_CODE
                    , CUST_ID
                    , CAST(SOURCE_TIMESTAMP AS DATE) AS SOURCE_TIMESTAMP
                    , FILM_NAME  
                FROM "STAGE_FILMS") AS SOURCE
            WHERE DW.SOURCE_TIMESTAMP!=SOURCE.SOURCE_TIMESTAMP              --NULL SHOULDN'T BE CONSIDERED WHEN USING THE != OPERATOR
                AND DW.FILM_NAME=SOURCE.FILM_NAME
                AND DW.POSTER_CONTENT=SOURCE.SHIP_NAMES
                AND DW.CUST_ID=SOURCE.CUST_ID;

        DROP TABLE "STAGE_FILMS";       
    """
    curr.execute(query)
    connect.commit()
    connect.close()
    
    return
    


    





def fetchFilms(end_point_start: str, posters: list):
    end_point = f'{end_point_start}'
    films = []
    ship_names=[]

    while True:

        try:
            r = requests.get(end_point)
            print(f'called {end_point}')
        except:
            raise ValueError(f'Unable to call API. Endpoint: {end_point}.')

        if r.status_code == 200:
            pass
        else:
            raise ValueError(f'The status code was {r.status_code} when 200 was expected.')

        data = r.json()

        for ship in data['results']:
            if ship['name'] not in posters:
                continue
            for film in ship['films']:  
                films.append(film)
                ship_names.append(ship['name'])

        end_point=data['next']

        if data['next'] is None:
            break
      
    return films,ship_names


def getCreatedDate(films: list):

    created_dates = []
    titles=[]
    film_url=[]

    if not films:
        raise ValueError(f'The films array is empty')

    for film in films:
        try:
            r=requests.get(film)
        except:
            raise ValueError(f'Unable to call API. Endpoint: {film}')

        if r.status_code==200:
            pass
        else:
            raise ValueError(f'The status code was {r.status_code} when 200 was expected.') 
        
        data=r.json()

        created_dates.append(data['release_date'])
        titles.append(data['title'])
        film_url.append(film) #link back to poster_content


    return created_dates, titles,film_url




    




    
if __name__ == '__main__':

    init_connected = connect(name=DB_NAME_DW, user=DB_USER_DW, pw=DB_PASS_DW, host=DB_HOST_DW, port=DB_PORT_DW) #connect to the database, pass connection into functions that connect to the database
    init_databases(conn=init_connected)

    #must run this again since the previous query closes the connection
    source_connected = connect(name=DB_NAME_DW, user=DB_USER_DW, pw=DB_PASS_DW, host=DB_HOST_DW, port=DB_PORT_DW) #connect to the database, pass connection into functions that connect to the database
    source_fetched=source_fetch(conn=source_connected) #returns source database


    unique_poster = []
    for result in source_fetched:
        if result[0] not in unique_poster: #dedup so only unique poster_content is fed into the starship API
            unique_poster.append(result[0])  #unique list of poster_content from the source_database

    film_urls, ship_names = fetchFilms(end_point_start=ENDPOINT, posters=unique_poster) #api contains unique ships. Feed in unique ships, and will return those ships and film_urls. Can contain duplicate film URLs and Ships, but combination of both are unique

    dedup_film_urls = list(dict.fromkeys(film_urls)) #dedup film urls
    release_date,film_name, film_url = getCreatedDate(dedup_film_urls) #api contains unique films. Feed in unique films, and will return the release_date, film_name, and film_url. Film Names/film URLs are unique.


    #DF from querying Starships API
    ship_df_ = {
        'film_url': film_urls,
        'ship_names': ship_names,
    }
    #DF from querying Films API
    film_df_ = {
        'release_date': release_date,
        'film_name': film_name,
        'film_url': film_url
    }

    ship_df = pd.DataFrame(ship_df_) #film_url+poster = id
    film_df = pd.DataFrame(film_df_) #unique films

    api_df=ship_df.merge(film_df, how="outer", on='film_url') #join (film_url, ship_name) + (release_Date, film_name, film_url) on film_url=film_url; - here we are joining on a list of unique film_urls to get the corresponding film_name and film_date. Won't work well for high volume data but works given the context



    #Take the result of querying the source database and put columns into lists (comes in as multi-dimensional array)
    poster_cust=[]
    price=[]
    sales_rep=[]
    promo_code=[]
    cust_id=[]
    source_timestamp=[]

    for cust in source_fetched:
        poster_cust.append(cust[0])
        price.append(cust[1])
        sales_rep.append(cust[2])
        promo_code.append(cust[3])
        cust_id.append(cust[4])
        source_timestamp.append(cust[5])

    
    #DF from source database   
    source_df_ = {
        'ship_names': poster_cust,
        'price': price,
        'sales_rep': sales_rep,
        'promo_code': promo_code,
        'cust_id': cust_id,
        'source_timestamp': source_timestamp
    }

    source_df = pd.DataFrame(source_df_)     #DF from source database

    df=api_df.merge(source_df, how='outer', on='ship_names') #merge all DFs. Won't scale well for high volume data but works given the context

    print(api_df)
    print(source_df)
    # print(df)

    #must run this again since the previous query closes the connection
    dw_connected = connect(name=DB_NAME_DW, user=DB_USER_DW, pw=DB_PASS_DW, host=DB_HOST_DW, port=DB_PORT_DW) #connect to the database, pass connection into functions that connect to the database
    query_dw(conn=dw_connected, df=df)



    



