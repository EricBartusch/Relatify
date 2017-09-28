"""
When run, this script will get all of the related artists (starting with Band Of Horses)
that Spotify has with a popularity rating over 20.
Puts data into a currently hardcoded, local neo4j graph database with their relationships
"""
import sys
import config
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from neo4j.v1 import GraphDatabase, basic_auth
CLIENT_CREDENTIALS_MANAGER = SpotifyClientCredentials(config.client, config.secret)
SPOTIFY = spotipy.Spotify(client_credentials_manager=CLIENT_CREDENTIALS_MANAGER)
ARTISTS_ARRAY = []
ARTISTS = set()
DRIVER = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", config.password))
SESSION = DRIVER.session()
SESSION.run("MATCH (n) DETACH DELETE n")

class Artist:
    """
    The object that gets added as a node into the neo4j database
    """
    def __init__(self, artist_id, name):
        self.artist_id = artist_id
        self.name = name

    def __eq__(self, other):
        return self.artist_id == other.artist_id

    def __hash__(self):
        return hash((self.artist_id, self.name))

def get_related_artists_recursive(artist_id):
    """
    Recursive version that will eventually hit python's recursion limit
    """
    #If we have processed this artist before, skip
    if artist_id in ARTISTS_ARRAY:
        return
    else:
        ARTISTS_ARRAY.append(artist_id)
        results = SPOTIFY.artist_related_artists(artist_id)
        for artist in results['artists']:
            related_artist_id = artist['id']
            name = artist['name']
            # if id not in artists:

            print(name + ' - ' + related_artist_id)
            SESSION.run("MERGE (a:Artist {name: {name}, id: {id}})", {"name": name, "id": related_artist_id})
            SESSION.run("MATCH (a:Artist),(b:Artist) WHERE a.id = '" + artist_id + "' AND b.id = '" + related_artist_id + "' CREATE (a) -[:relates_to]-> (b)")
            get_related_artists_recursive(related_artist_id)


def get_related_artists(artist_id):
    """
    Iterative version.  Uses the global set to keep track of artists to process
    Will process an artist (if over popularity 20) by getting their related artists, adding those to the set,
    then adds/merges artist node and creates relationship
    """
    first_artist_json = SPOTIFY.artist(artist_id)
    first_artist = Artist(first_artist_json['id'], first_artist_json['name'])
    SESSION.run("MERGE (a:Artist {name: {name}, id: {id}})", {"name": first_artist.name, "id": first_artist.artist_id})
    ARTISTS.add(first_artist)
    i = 0
    while 1:
        if i == len(ARTISTS):
            return
        artist = ARTISTS.pop()
        print(artist.name + ' - ' + artist.artist_id)
        
        results = SPOTIFY.artist_related_artists(artist.artist_id)
        for related_artist in results['artists']:
            related_artist_id = related_artist['id']
            related_artist_name = related_artist['name']
            if related_artist['popularity'] > 20:
                ARTISTS.add(Artist(related_artist_id, related_artist_name))

            SESSION.run("MERGE (a:Artist {name: {name}, id: {id}})", {"name": related_artist_name, "id": related_artist_id})
            SESSION.run("MATCH (a:Artist),(b:Artist) WHERE a.id = '" + artist.artist_id + "' AND b.id = '" + related_artist_id + "' CREATE (a) -[:relates_to]-> (b)")
        i = i + 1

#0OdUWJ0sBjDrqHygGUXeCF - Band Of Horses
if len(sys.argv) < 2:
    print("Must pass in an artist id")
else:
    get_related_artists(sys.argv[1])
    print("complete")
