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
import numpy


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
        self.artistFamiliarity = None
        self.artistHotttnesss = None
        self.artistmbid = None
        self.artistPlaymeid = None
        self.artist7digitalid = None
        self.artistTerms = None

        self.artistTermsCount = None
        self.artistTermsFreq = None
        self.artistTermsWeight = None
        self.artistMBTags = None

        self.artistMBTagsOuterCount = None
        self.artistMBTagsCount = None
        self.analysisSampleRate = None
        self.audioMD5 = None
        self.endOfFadeIn = None
        self.startOfFadeOut = None
        self.energy = None
        self.release = None
        self.release7digitalid = None
        self.songHotness = None
        self.track7digitalid = None
        self.similarartists = None

        self.similarArtistsCount = None
        self.loudness = None
        self.mode = None
        self.modeConfidence = None
        self.artistName = None
        self.danceability = None
        self.duration = None
        self.keySignature = None
        self.keySignatureConfidence = None
        self.tempo = None
        self.timeSignature = None
        self.timeSignatureConfidence = None
        self.title = None
        self.year = None
        self.trackID = None
        self.segmentsStart = None

        self.segmentsCount = None
        self.segmentsConfidence = None
        self.segmentsPitches = None
        self.segmentsTimbre = None
        self.segmentsLoudnessMax = None
        self.segmentsLoudnessMaxTime = None
        self.segmentsLoudnessStart = None
        self.sectionStarts = None

        self.sectionCount = None
        self.sectionsConfidence = None
        self.beatsStart = None

        self.beatsCount = None
        self.beatsConfidence = None
        self.barsStart = None

        self.barsCount = None
        self.barsConfidence = None

        self.tatumsStart = None
        self.tatumsCount = None
        self.tatumsConfidence = None

    def displaySongCount(self):
        print "Total Song Count %i" % Song.songCount

    def displaySong(self):
        print "ID: %s" % self.id


def get_list_length(lst):
    """
    Gets the length of a list or returns 1 in case the lst object is not a list

    :param lst: the list
    :return: the list's length
    """
    x = 1

    try:
        x = len(lst)
    except:
        pass

    return str(x)


def remove_trap_characters(pre_text):
    return pre_text.replace('\n', '').replace(';', '-')


def writeheader(write_file, scv_row_string, sep=','):
    # write_file.write('sep=;\n')
    write_file.write("SongNumber" + sep)
    write_file.write(scv_row_string + "\n")


def parse_nested_list(lst):
    lst = list(lst)
    return [list(x) for x in lst]


if __name__ == '__main__':
    # Default values for these
    output_dir = './out'
    base_dir = '.'
    base_file_name = 'SongCSV_aux{0}.csv'

    sep = ';'

    ext = ".h5"  # Set the extension here. H5 is the extension for HDF5 files.

    print(sys.argv)

    # First check the arguments
    if len(sys.argv) > 2:
        base_dir = sys.argv[1]
        output_dir = sys.argv[2]
    elif len(sys.argv) > 1:
        base_dir = sys.argv[1]

        print "Did not receive a second input argument; defaulting output dir to './out/'"
        print "Usage: python msdHDF5toCSV.py <h5_base_dir> <output_dir>"
    else:
        print "Did not receive any input arguments; defaulting base_dir to '.'and output_dir to './out/'"
        print "Usage: python msdHDF5toCSV.py <h5_base_dir> <output_dir>"

    numpy.set_printoptions(threshold=sys.maxint)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    csvHeaderString = (
        "SongID;albumName;albumID;artistID;artistLatitude;artistLocation;artistLongitude;artistFamiliarity;"
        "artistHotttnesss;artistmbid;artistPlaymeid;artist7digitalid;artistTerms;artistTermsCount;artistTermsFreq;"
        "artistTermsWeight; artistMBTags;artistMBTagsOuterCount;artistMBTagsCount;analysisSampleRate;audioMD5;endOfFadeIn;"
        "startOfFadeOut;energy;release;release7digitalid;songHotness;track7digitalid;similarartists;similarArtistsCount;"
        "loudness;mode;modeConfidence;artistName;danceability;duration;keySignature;keySignatureConfidence;tempo;"
        "timeSignature;timeSignatureConfidence;title;year;trackID;segmentsStart;segmentsCount;segmentsConfidence;"
        "segmentsPitches;segmentsTimbre;segmentsLoudnessMax;segmentsLoudnessMaxTime;segmentsLoudnessStart;sectionStarts;"
        "sectionCount;sectionsConfidence;beatsStart;beatsCount;beatsConfidence;barsStart;barsCount;barsConfidence;"
        "tatumsStart;tatumsCount;tatumsConfidence"
    )
    # csvHeaderString = (
    #     "SongID;albumName;albumID;artistID;artistLatitude;artistLocation;artistLongitude;artistFamiliarity;"
    #     "artistHotttnesss;artistmbid;artistPlaymeid;artist7digitalid;artistTermsCount;"
    #     "artistMBTagsOuterCount;analysisSampleRate;audioMD5;endOfFadeIn;"
    #     "startOfFadeOut;energy;release;release7digitalid;songHotness;track7digitalid;similarArtistsCount;"
    #     "loudness;mode;modeConfidence;artistName;danceability;duration;keySignature;keySignatureConfidence;tempo;"
    #     "timeSignature;timeSignatureConfidence;title;year;trackID;segmentsCount;"
    #     "sectionCount;beatsCount;barsCount;"
    #     "tatumsCount"
    # )

    # csvAttributeList = re.split('\W+', csvHeaderString)
    # for i, v in enumerate(csvAttributeList):
    #     csvAttributeList[i] = v.lower()

    outputFile1 = open(os.path.join(output_dir, base_file_name).format(0), "w")

    writeheader(outputFile1, csvHeaderString, sep)
    print("started converting")

    # Set the basedir here, the root directory from which the search
    # for files stored in a (hierarchical data structure) will originate
    #################################################

    # FOR LOOP
    csvRowString = ""
    row_limit = 10000

    counter = 0
    file_counter = 0
    for root, dirs, files in os.walk(base_dir):
        fs = glob.glob(os.path.join(root, '*' + ext))
        for f in fs:
            counter += 1
            if (counter % 1000) == 0:
                print "Progress: {0}".format(counter)
            if (counter % 10000) == 0:
                outputFile1.close()
                file_counter += 1
                outputFile1 = open(os.path.join(output_dir, base_file_name).format(file_counter), 'w')
                writeheader(outputFile1, csvHeaderString, ';')

            songH5File = hdf5_getters.open_h5_file_read(f)
            song = Song(str(hdf5_getters.get_song_id(songH5File)))

            testDanceability = hdf5_getters.get_danceability(songH5File)
            # print type(testDanceability)
            # print ("Here is the danceability: ") + str(testDanceability)

            song.artistName = remove_trap_characters(str(hdf5_getters.get_artist_name(songH5File)))
            song.artistID = remove_trap_characters(str(hdf5_getters.get_artist_id(songH5File)))
            song.albumID = remove_trap_characters(str(hdf5_getters.get_release_7digitalid(songH5File)))
            song.artistLatitude = remove_trap_characters(str(hdf5_getters.get_artist_latitude(songH5File)))
            # Replace the comma in the location (if there is one), since this will displace the entire row
            song.artistLocation = remove_trap_characters(
                str(hdf5_getters.get_artist_location(songH5File))
            ).replace(',', ':')
            song.artistLongitude = remove_trap_characters(str(hdf5_getters.get_artist_longitude(songH5File)))
            song.artistFamiliarity = remove_trap_characters(str(hdf5_getters.get_artist_familiarity(songH5File)))
            song.artistHotttnesss = remove_trap_characters(str(hdf5_getters.get_artist_hotttnesss(songH5File)))
            song.artistmbid = remove_trap_characters(str(hdf5_getters.get_artist_mbid(songH5File)))
            song.artistPlaymeid = remove_trap_characters(str(hdf5_getters.get_artist_playmeid(songH5File)))
            song.artist7digitalid = remove_trap_characters(str(hdf5_getters.get_artist_7digitalid(songH5File)))

            temp = hdf5_getters.get_artist_terms(songH5File)
            song.artistTerms = remove_trap_characters(str(list(temp)))
            song.artistTermsCount = get_list_length(temp)
            song.artistTermsFreq = remove_trap_characters(str(list(hdf5_getters.get_artist_terms_freq(songH5File))))
            song.artistTermsWeight = remove_trap_characters(str(list(hdf5_getters.get_artist_terms_weight(songH5File))))

            temp = hdf5_getters.get_artist_mbtags(songH5File)
            song.artistMBTags = remove_trap_characters(str(list(temp)))
            song.artistMBTagsOuterCount = get_list_length(temp)
            song.artistMBTagsCount = remove_trap_characters(str(list(hdf5_getters.get_artist_mbtags_count(songH5File))))
            song.analysisSampleRate = remove_trap_characters(str(hdf5_getters.get_analysis_sample_rate(songH5File)))
            song.audioMD5 = remove_trap_characters(str(hdf5_getters.get_audio_md5(songH5File)))
            song.endOfFadeIn = remove_trap_characters(str(hdf5_getters.get_end_of_fade_in(songH5File)))
            song.startOfFadeOut = remove_trap_characters(str(hdf5_getters.get_start_of_fade_out(songH5File)))
            song.energy = remove_trap_characters(str(hdf5_getters.get_energy(songH5File)))
            song.release = remove_trap_characters(str(hdf5_getters.get_release(songH5File)))
            song.release7digitalid = remove_trap_characters(str(hdf5_getters.get_release_7digitalid(songH5File)))
            song.songHotness = remove_trap_characters(str(hdf5_getters.get_song_hotttnesss(songH5File)))
            song.track7digitalid = remove_trap_characters(str(hdf5_getters.get_track_7digitalid(songH5File)))

            temp = hdf5_getters.get_similar_artists(songH5File)
            song.similarartists = remove_trap_characters(str(list(list(temp))))
            song.similarArtistsCount = get_list_length(temp)
            song.loudness = remove_trap_characters(str(hdf5_getters.get_loudness(songH5File)))
            song.mode = remove_trap_characters(str(hdf5_getters.get_mode(songH5File)))
            song.modeConfidence = remove_trap_characters(str(hdf5_getters.get_mode_confidence(songH5File)))
            song.artistName = remove_trap_characters(str(hdf5_getters.get_artist_name(songH5File)))
            song.danceability = remove_trap_characters(str(hdf5_getters.get_danceability(songH5File)))
            song.duration = remove_trap_characters(str(hdf5_getters.get_duration(songH5File)))
            song.keySignature = remove_trap_characters(str(hdf5_getters.get_key(songH5File)))
            song.keySignatureConfidence = remove_trap_characters(str(hdf5_getters.get_key_confidence(songH5File)))
            song.tempo = remove_trap_characters(str(hdf5_getters.get_tempo(songH5File)))
            song.timeSignature = remove_trap_characters(str(hdf5_getters.get_time_signature(songH5File)))
            song.timeSignatureConfidence = remove_trap_characters(str(hdf5_getters.get_time_signature_confidence(songH5File)))
            song.title = remove_trap_characters(str(hdf5_getters.get_title(songH5File)))
            song.year = remove_trap_characters(str(hdf5_getters.get_year(songH5File)))
            song.trackID = remove_trap_characters(str(hdf5_getters.get_track_id(songH5File)))

            temp = hdf5_getters.get_segments_start(songH5File)
            song.segmentsStart = remove_trap_characters(str(list(temp)))
            song.segmentsCount = get_list_length(temp)
            song.segmentsConfidence = \
                remove_trap_characters(str(list(hdf5_getters.get_segments_confidence(songH5File))))
            # song.segmentsPitches = remove_trap_characters(str(list(hdf5_getters.get_segments_pitches(songH5File))))
            song.segmentsPitches = \
                remove_trap_characters(str(parse_nested_list(hdf5_getters.get_segments_pitches(songH5File))))
            # song.segmentsTimbre = remove_trap_characters(str(list(hdf5_getters.get_segments_timbre(songH5File))))
            song.segmentsTimbre = \
                remove_trap_characters(str(parse_nested_list(hdf5_getters.get_segments_timbre(songH5File))))
            song.segmentsLoudnessMax = \
                remove_trap_characters(str(list(hdf5_getters.get_segments_loudness_max(songH5File))))
            song.segmentsLoudnessMaxTime = \
                remove_trap_characters(str(list(hdf5_getters.get_segments_loudness_max_time(songH5File))))
            song.segmentsLoudnessStart = \
                remove_trap_characters(str(list(hdf5_getters.get_segments_loudness_start(songH5File))))

            temp = hdf5_getters.get_sections_start(songH5File)
            song.sectionStarts = remove_trap_characters(str(list(temp)))
            song.sectionCount = get_list_length(temp)
            song.sectionsConfidence = remove_trap_characters(str(list(hdf5_getters.get_sections_confidence(songH5File))))

            temp = hdf5_getters.get_beats_start(songH5File)
            song.beatsStart = remove_trap_characters(str(list(temp)))
            song.beatsCount = get_list_length(temp)
            song.beatsConfidence = remove_trap_characters(str(list(hdf5_getters.get_beats_confidence(songH5File))))

            temp = hdf5_getters.get_bars_start(songH5File)
            song.barsStart = remove_trap_characters(str(list(temp)))
            song.barsCount = get_list_length(temp)
            song.barsConfidence = remove_trap_characters(str(list(hdf5_getters.get_bars_confidence(songH5File))))

            temp = hdf5_getters.get_tatums_start(songH5File)
            song.tatumsStart = remove_trap_characters(str(list(temp)))
            song.tatumsCount = get_list_length(temp)
            song.tatumsConfidence = remove_trap_characters(str(list(hdf5_getters.get_tatums_confidence(songH5File))))

            # print song count
            csvRowString += str(song.songCount) + sep

            for attribute in re.split('\W+', csvHeaderString):
                # print "Here is the attribute: " + attribute + " \n"
                if attribute != "SongID":
                    csvRowString += str(getattr(song, attribute))
                elif attribute != "SongID":
                    print "Attibute {0} not found".format(attribute)

                csvRowString += sep

            # Remove the final comma from each row in the csv
            lastIndex = len(csvRowString)
            csvRowString = csvRowString[0:lastIndex - 1]
            csvRowString += "\n"
            outputFile1.write(csvRowString)
            csvRowString = ""

            songH5File.close()

    outputFile1.close()
