import spotipy
import config
from spotipy.oauth2 import SpotifyClientCredentials
import json
from neo4j.v1 import GraphDatabase, basic_auth
client_credentials_manager = SpotifyClientCredentials(config.client, config.secret)
spotify = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
artists = []
driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", config.password))
session = driver.session()
session.run("MATCH (n) DETACH DELETE n")

def getRelatedArtists(artistId):
    #If we have processed this artist before, skip
    if artistId in artists:
        return
    else:
        artists.append(artistId)
        results = spotify.artist_related_artists(artistId)
        for x in results['artists']:
            id = x['id']
            name = x['name']
            # if id not in artists:

            print(name + ' - ' + id)
            session.run("MERGE (a:Artist {name: {name}, id: {id}})", {"name": name, "id": id})
            session.run("MATCH (a:Artist),(b:Artist) WHERE a.id = '" + artistId + "' AND b.id = '" + id + "' CREATE (a)-[r:RELTYPE { name: a.id + '->' + b.name }]->(b) RETURN r")
            getRelatedArtists(id)

getRelatedArtists('0OdUWJ0sBjDrqHygGUXeCF')

            