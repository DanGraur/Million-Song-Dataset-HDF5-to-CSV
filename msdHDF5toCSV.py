"""
Alexis Greenstreet (October 4, 2015) University of Wisconsin-Madison

This code is designed to convert the HDF5 files of the Million Song Dataset
to a CSV by extracting various song properties.

The script writes to a "SongCSV.csv" in the directory containing this script.

Please note that in the current form, this code only extracts the following
information from the HDF5 files:
AlbumID, AlbumName, ArtistID, ArtistLatitude, ArtistLocation,
ArtistLongitude, ArtistName, Danceability, Duration, KeySignature,
KeySignatureConfidence, SongID, Tempo, TimeSignature,
TimeSignatureConfidence, Title, and Year.

This file also requires the use of "hdf5_getters.py", written by
Thierry Bertin-Mahieux (2010) at Columbia University

Credit:
This HDF5 to CSV code makes use of the following example code provided
at the Million Song Dataset website 
(Home>Tutorial/Iterate Over All Songs, 
http://labrosa.ee.columbia.edu/millionsong/pages/iterate-over-all-songs),
Which gives users the following code to get all song titles:

import os
import glob
import hdf5_getters
def get_all_titles(basedir,ext='.h5') :
    titles = []
    for root, dirs, files in os.walk(basedir):
        files = glob.glob(os.path.join(root,'*'+ext))
        for f in files:
            h5 = hdf5_getters.open_h5_file_read(f)
            titles.append( hdf5_getters.get_title(h5) )
            h5.close()
    return titles
"""

import sys
import os
import glob
import hdf5_getters
import re

class Song:
    songCount = 0
    # songDictionary = {}

    def __init__(self, songID):
        self.id = songID
        Song.songCount += 1
        # Song.songDictionary[songID] = self

        self.albumName = None
        self.albumID = None
        self.artistID = None
        self.artistLatitude = None
        self.artistLocation = None
        self.artistLongitude = None
        self.artistName = None
        self.danceability = None
        self.duration = None
        self.genreList = []
        self.keySignature = None
        self.keySignatureConfidence = None
        self.lyrics = None
        self.popularity = None
        self.tempo = None
        self.timeSignature = None
        self.timeSignatureConfidence = None
        self.title = None
        self.year = None

    def displaySongCount(self):
        print "Total Song Count %i" % Song.songCount

    def displaySong(self):
        print "ID: %s" % self.id  
	
def writeheader(file):
	csvRowString = ("SongID,AlbumID,AlbumName,ArtistID,ArtistLatitude,ArtistLocation,"+
		"ArtistLongitude,ArtistName,Danceability,Duration,KeySignature,"+
		"KeySignatureConfidence,Tempo,TimeSignature,TimeSignatureConfidence,"+
		"Title,Year")
	csvAttributeList = re.split('\W+', csvRowString)
	for i, v in enumerate(csvAttributeList):
		csvAttributeList[i] = csvAttributeList[i].lower()
	file.write("SongNumber,");
	file.write(csvRowString + "\n");
	csvRowString = ""  

def main():
    if not os.path.exists("out"):
    	os.makedirs("out")
    outputFile1 = open('out/SongCSV0.csv', 'w')
    csvRowString = ""
    writeheader(outputFile1)
    print("started converting")



    #Set the basedir here, the root directory from which the search
    #for files stored in a (hierarchical data structure) will originate
    basedir = "." # "." As the default means the current directory
    ext = ".h5" #Set the extension here. H5 is the extension for HDF5 files.
    #################################################

    #FOR LOOP
    counter = 0
    filecounter = 0
    for root, dirs, files in os.walk(basedir):        
        files = glob.glob(os.path.join(root,'*'+ext))
        for f in files:
	    counter+=1
	    if (counter % 10000) == 0 :
	    	print "Progress: {0}".format(counter)
    		outputFile1.close()
		filecounter+=1
		outputFile1 = open('out/SongCSV{0}.csv'.format(filecounter), 'w')
		writeheader(outputFile1)
		
            songH5File = hdf5_getters.open_h5_file_read(f)
            song = Song(str(hdf5_getters.get_song_id(songH5File)))

            testDanceability = hdf5_getters.get_danceability(songH5File)
            # print type(testDanceability)
            # print ("Here is the danceability: ") + str(testDanceability)

            song.artistID = str(hdf5_getters.get_artist_id(songH5File))
            song.albumID = str(hdf5_getters.get_release_7digitalid(songH5File))
            song.albumName = str(hdf5_getters.get_release(songH5File))
            song.artistLatitude = str(hdf5_getters.get_artist_latitude(songH5File))
            song.artistLocation = str(hdf5_getters.get_artist_location(songH5File))
            song.artistLongitude = str(hdf5_getters.get_artist_longitude(songH5File))
            song.artistName = str(hdf5_getters.get_artist_name(songH5File))
            song.danceability = str(hdf5_getters.get_danceability(songH5File))
            song.duration = str(hdf5_getters.get_duration(songH5File))
            # song.setGenreList()
            song.keySignature = str(hdf5_getters.get_key(songH5File))
            song.keySignatureConfidence = str(hdf5_getters.get_key_confidence(songH5File))
            # song.lyrics = None
            # song.popularity = None
            song.tempo = str(hdf5_getters.get_tempo(songH5File))
            song.timeSignature = str(hdf5_getters.get_time_signature(songH5File))
            song.timeSignatureConfidence = str(hdf5_getters.get_time_signature_confidence(songH5File))
            song.title = str(hdf5_getters.get_title(songH5File))
            song.year = str(hdf5_getters.get_year(songH5File))

            #print song count
            csvRowString += str(song.songCount) + ","

            for attribute in csvAttributeList:
                # print "Here is the attribute: " + attribute + " \n"

                if attribute == 'AlbumID'.lower():
                    csvRowString += song.albumID
                elif attribute == 'AlbumName'.lower():
                    albumName = song.albumName
                    albumName = albumName.replace(',',"")
                    csvRowString += "\"" + albumName + "\""
                elif attribute == 'ArtistID'.lower():
                    csvRowString += "\"" + song.artistID + "\""
                elif attribute == 'ArtistLatitude'.lower():
                    latitude = song.artistLatitude
                    if latitude == 'nan':
                        latitude = ''
                    csvRowString += latitude
                elif attribute == 'ArtistLocation'.lower():
                    location = song.artistLocation
                    location = location.replace(',','')
                    csvRowString += "\"" + location + "\""
                elif attribute == 'ArtistLongitude'.lower():
                    longitude = song.artistLongitude
                    if longitude == 'nan':
                        longitude = ''
                    csvRowString += longitude                
                elif attribute == 'ArtistName'.lower():
                    csvRowString += "\"" + song.artistName + "\""                
                elif attribute == 'Danceability'.lower():
                    csvRowString += song.danceability
                elif attribute == 'Duration'.lower():
                    csvRowString += song.duration
                elif attribute == 'KeySignature'.lower():
                    csvRowString += song.keySignature
                elif attribute == 'KeySignatureConfidence'.lower():
                    # print "key sig conf: " + song.timeSignatureConfidence                                 
                    csvRowString += song.keySignatureConfidence
                elif attribute == 'SongID'.lower():
                    csvRowString += "\"" + song.id + "\""
                elif attribute == 'Tempo'.lower():
                    # print "Tempo: " + song.tempo
                    csvRowString += song.tempo
                elif attribute == 'TimeSignature'.lower():
                    csvRowString += song.timeSignature
                elif attribute == 'TimeSignatureConfidence'.lower():
                    # print "time sig conf: " + song.timeSignatureConfidence                                   
                    csvRowString += song.timeSignatureConfidence
                elif attribute == 'Title'.lower():
                    csvRowString += "\"" + song.title + "\""
                elif attribute == 'Year'.lower():
                    csvRowString += song.year
                else:
                    csvRowString += "Erm. This didn't work. Error. :( :(\n"

                csvRowString += ","

            #Remove the final comma from each row in the csv
            lastIndex = len(csvRowString)
            csvRowString = csvRowString[0:lastIndex-1]
            csvRowString += "\n"
            outputFile1.write(csvRowString)
            csvRowString = ""

            songH5File.close()

    outputFile1.close()
	
main()
