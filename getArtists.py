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

class Artist:
    def __init__(self, id, name):
        self.id = id
        self.name = name

    def __eq__(self, other):
        return self.id == other.id

def getRelatedArtists(artistId):
    #If we have processed this artist before, skip
    if artistId in artists:
        return
    else:
        artists.append(artistId)
        results = spotify.artist_related_artists(artistId)
        for x in results['artists']:
            relatedArtistId = x['id']
            name = x['name']
            # if id not in artists:

            print(name + ' - ' + relatedArtistId)
            session.run("MERGE (a:Artist {name: {name}, id: {id}})", {"name": name, "id": relatedArtistId})
            session.run("MATCH (a:Artist),(b:Artist) WHERE a.id = '" + artistId + "' AND b.id = '" + relatedArtistId + "' CREATE (a) -[:relates_to]-> (b)")
            getRelatedArtists(relatedArtistId)


def getRelatedArtistsIterative(artistId):
    firstArtistJson = spotify.artist(artistId)
    firstArtist = Artist(firstArtistJson['id'], firstArtistJson['name'])
    session.run("MERGE (a:Artist {name: {name}, id: {id}})", {"name": firstArtist.name, "id": firstArtist.id})
    artists.append(firstArtist)
    i = 0
    while 1:
        if i == len(artists):
            return
        results = spotify.artist_related_artists(artists[i].id)
        for x in results['artists']:
            relatedArtist = Artist(x['id'], x['name'])
            if relatedArtist not in artists:
                artists.append(relatedArtist)

        print(artists[i].name + ' - ' + artists[i].id)
        for relatedArtist in results['artists']:
            relatedArtistId = relatedArtist['id']
            relatedArtistName = relatedArtist['name']
            session.run("MERGE (a:Artist {name: {name}, id: {id}})", {"name": relatedArtistName, "id": relatedArtistId})
            session.run("MATCH (a:Artist),(b:Artist) WHERE a.id = '" + artists[i].id + "' AND b.id = '" + relatedArtistId + "' CREATE (a) -[:relates_to]-> (b)")
        i = i + 1

getRelatedArtistsIterative('0OdUWJ0sBjDrqHygGUXeCF')     