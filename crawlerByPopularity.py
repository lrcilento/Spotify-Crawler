import spotipy
import pandas as pd
import mysql.connector
import json
from spotipy.oauth2 import SpotifyClientCredentials
from credentials import cid, secret
from datetime import date

db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="passwd",
    database="Spotify"
)

day = date.today().strftime("%d")

month = date.today().strftime("%B")

maximumPopularity = 100

minimumPopularity = 50

sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=cid, client_secret=secret))

cursor = db.cursor()

sql = """CREATE TABLE {0} (
    track_id VARCHAR(45) PRIMARY KEY NOT NULL,
    track_name VARCHAR(90) DEFAULT NULL,
    album_name VARCHAR(90) DEFAULT NULL,
    artist_name VARCHAR(90) DEFAULT NULL,
    year INT DEFAULT NULL,
    genres JSON DEFAULT NULL,
    popularity INT DEFAULT NULL)""".format(month+str(day))

try:

    cursor.execute(sql)

    db.commit()

    print("Tabela de "+date.today().strftime("%B%d")+" criada. Iniciando requisição.")

    popularity = maximumPopularity

    while popularity >= minimumPopularity:

        artist_name = []
        track_name = []
        year = []
        genre = []
        artist_id = []
        album = []
        track_id = []

        for i in range(0, 1999, 50):
            track_results = sp.search(q='popularity:{0}'.format(popularity), type='track', limit=50, offset=i)
            for i, t in enumerate(track_results['tracks']['items']):
                artist_name.append(t['artists'][0]['name'])
                track_name.append(t['name'])
                year.append(t['year'])
                album.append(t['album']['name'])
                track_id.append(t['id'])
                artist_id.append(t['artists'][0]['id'])

        for i in range(len(artist_id)):
            track_genre = sp.artist(artist_id=artist_id[i])['genres']
            genre.append(track_genre)

            sql = "INSERT INTO {0} (track_id, track_name, album_name, artist_name, genres, popularity, year) VALUES (%s, %s, %s, %s, %s, %s, %s)".format(month+str(day))
            val = (track_id[i], track_name[i], album[i], artist_name[i], json.dumps(genre[i]), popularity, year[i])

            try:

                cursor.execute(sql, val)

                db.commit()

            except mysql.connector.Error as err:

                print("Erro ao inserir registro: {}".format(err))
        
        print(str((maximumPopularity/minimumPopularity)*(maximumPopularity-popularity)) + "% concluído.")
        popularity = popularity - 1

except mysql.connector.Error as err:

    print("Erro ao criar tabela: {}".format(err))