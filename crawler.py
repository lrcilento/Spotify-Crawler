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

year = 2020

finalYear = 1880

minimumPopularity = 50

created = False

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

    created = True

except mysql.connector.Error as err:

    print("Erro ao criar tabela: {}".format(err))
    answer = input("Gostaria de prosseguir? (y/N)")
    if answer == 'y' or answer == 'Y':
        create = True

if created == True:

    while year >= finalYear:

        dup_count = 0
        unpopular = 0
        insert_count = 0
        dup = None
        artist_name = []
        track_name = []
        popularity = []
        genre = []
        artist_id = []
        album = []
        track_id = []

        print("Iniciando requisição para "+str(year)+".")

        for i in range(0, 1999, 50):
            track_results = sp.search(q='year:{0}'.format(year), type='track', limit=50, offset=i)
            for i, t in enumerate(track_results['tracks']['items']):
                artist_name.append(t['artists'][0]['name'])
                track_name.append(t['name'])
                popularity.append(t['popularity'])
                album.append(t['album']['name'])
                track_id.append(t['id'])
                artist_id.append(t['artists'][0]['id'])

        print("Requisição concluída. Iniciando formatação.")

        for i in range(len(artist_id)):

            if popularity[i] >= minimumPopularity:

                sql = "SELECT track_name, artist_name, popularity FROM {0} WHERE track_name = %s AND artist_name = %s".format(month+str(day))
                val = (track_name[i], artist_name[i])

                try:

                    cursor.execute(sql, val)
                    dup = cursor.fetchone()

                    if dup == None:
                        
                        track_genre = sp.artist(artist_id=artist_id[i])['genres']
                        genre.append(track_genre)

                        sql = "INSERT INTO {0} (track_id, track_name, album_name, artist_name, genres, popularity, year) VALUES (%s, %s, %s, %s, %s, %s, %s)".format(month+str(day))
                        val = (track_id[i], track_name[i], album[i], artist_name[i], json.dumps(genre[i]), popularity[i], year)

                        try:

                            cursor.execute(sql, val)

                            db.commit()

                            insert_count = insert_count + 1

                        except mysql.connector.Error as err:

                            print("Erro ao inserir registro: {}".format(err))

                    else:

                        if dup[2] < popularity[i]:

                            track_genre = sp.artist(artist_id=artist_id[i])['genres']
                            genre.append(track_genre)

                            print("Duplicidade de maior popularidade encontrada: "+track_name[i]+" de "+artist_name[i]+". Atualizando.")

                            sql = "UPDATE {0} SET popularity = %s WHERE track_name = %s AND artist_name = %s".format(month+str(day))
                            val = (popularity[i], track_name[i], artist_name[i])

                            try:

                                cursor.execute(sql, val)

                                db.commit()

                            except mysql.connector.Error as err:

                                print("Erro ao atualizar registro: {}".format(err))

                        else:

                            genre.append([])

                            print("Duplicidade de menor ou igual popularidade encontrada: "+track_name[i]+" de "+artist_name[i]+". Ignorando.")

                        dup_count = dup_count + 1

                except mysql.connector.Error as err:

                    print("Erro ao verificar duplicidade: {}".format(err))
            
            else:

                genre.append([])
                unpopular = unpopular + 1
        
        print(str(year) + ' concluído. '+str(2000-dup_count-unpopular)+' músicas registradas. '+str(unpopular)+' de baixa popularidade e '+str(dup_count)+' duplicidades ignoradas.')
        year = year - 1