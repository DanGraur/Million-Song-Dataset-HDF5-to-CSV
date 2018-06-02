import os
import sys
import uuid

import numpy as np
from cassandra.cluster import Cluster


def get_list_length(lst):
    """
    Gets the length of a list or returns 1 in case the lst object is not a list

    :param lst: the list
    :return: the list's length
    """
    x_inner = 1

    try:
        x_inner = len(lst)
    except:
        pass

    return str(x_inner)


def remove_trap_characters(pre_text):
    return pre_text.replace('\n', '').replace(';', '-')


def writeheader(write_file, scv_row_string, sep=','):
    # write_file.write('sep=;\n')
    write_file.write("SongNumber" + sep)
    write_file.write(scv_row_string + "\n")


def parse_nested_list(lst):
    lst = list(lst)
    return [list(x) for x in lst]


def from_string_to_list(lst):
    return list(np.fromstring(lst[1:-1], sep=' '))


if __name__ == '__main__':
    # Default values for these
    base_dir = './out'

    sep = ','

    ext = ".csv"  # Set the extension here. H5 is the extension for HDF5 files.

    print(sys.argv)

    # First check the arguments
    if len(sys.argv) > 1:
        base_dir = sys.argv[1]
    else:
        print "Did not receive any input arguments; defaulting base_dir to './out'"
        print "Usage: python CSVtoImport.py <base_dir>"

    print("Started converting")

    #################################################

    # FOR LOOP
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()

    ADD_ROW_PREPARED = """
        INSERT INTO million_song.song_table_full (
            SongNumber,
            SongID,
            albumName,
            albumID,
            artistID,
            artistLatitude,
            artistLocation,
            artistLongitude,
            artistFamiliarity,
            artistHotttnesss,
            artistmbid,
            artistPlaymeid,
            artist7digitalid,
            artistTerms,
            artistTermsCount,
            artistTermsFreq,
            artistTermsWeight,
            artistMBTags,
            artistMBTagsOuterCount,
            artistMBTagsCount,
            analysisSampleRate,
            audioMD5,
            endOfFadeIn,
            startOfFadeOut,
            energy,
            release,
            release7digitalid,
            songHotness,
            track7digitalid,
            similarartists,
            similarArtistsCount,
            loudness,
            mode,
            modeConfidence,
            artistName,
            danceability,
            duration,
            keySignature,
            keySignatureConfidence,
            tempo,
            timeSignature,
            timeSignatureConfidence,
            title,
            year,
            trackID,
            segmentsStart,
            segmentsCount,
            segmentsConfidence,
            segmentsPitches,
            segmentsTimbre,
            segmentsLoudnessMax,
            segmentsLoudnessMaxTime,
            segmentsLoudnessStart,
            sectionStarts,
            sectionCount,
            sectionsConfidence,
            beatsStart,
            beatsCount,
            beatsConfidence,
            barsStart,
            barsCount,
            barsConfidence,
            tatumsStart,
            tatumsCount,
            tatumsConfidence
        ) VALUES (
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?,
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?, 
            ?
        )
    """

    # Prepare the DB
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS million_song
        WITH REPLICATION = { 
       'class' : 'SimpleStrategy', 
       'replication_factor' : 1 
        }
        """
    )

    session.execute(
        """
        DROP TABLE IF EXISTS million_song.song_table_full
        """
    )

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS million_song.song_table_full (
            SongNumber bigint,
            SongID text,
            albumName text,
            albumID bigint,
            artistID text,
            artistLatitude float,
            artistLocation text,
            artistLongitude float,
            artistFamiliarity double,
            artistHotttnesss double,
            artistmbid uuid,
            artistPlaymeid bigint,
            artist7digitalid bigint,
            artistTerms list<text>,
            artistTermsCount float,
            artistTermsFreq list<double>,
            artistTermsWeight list<double>,
            artistMBTags list<text>,
            artistMBTagsOuterCount int,
            artistMBTagsCount list<float>,
            analysisSampleRate int,
            audioMD5 text,
            endOfFadeIn float,
            startOfFadeOut float,
            energy float,
            release text,
            release7digitalid bigint,
            songHotness double,
            track7digitalid bigint,
            similarartists list<text>,
            similarArtistsCount int,
            loudness float,
            mode int,
            modeConfidence float,
            artistName text,
            danceability float,
            duration float,
            keySignature int,
            keySignatureConfidence float,
            tempo float,
            timeSignature int,
            timeSignatureConfidence float,
            title text,
            year int,
            trackID text,
            segmentsStart list<float>,
            segmentsCount int,
            segmentsConfidence list<float>,
            segmentsPitches list<frozen <list<float>>>,
            segmentsTimbre list<frozen <list<float>>>,
            segmentsLoudnessMax list<float>,
            segmentsLoudnessMaxTime list<float>,
            segmentsLoudnessStart list<float>,
            sectionStarts list<float>,
            sectionCount int,
            sectionsConfidence list<float>,
            beatsStart list<float>,
            beatsCount int,
            beatsConfidence list<float>,
            barsStart list<float>,
            barsCount int,
            barsConfidence list<float>,
            tatumsStart list<float>,
            tatumsCount int,
            tatumsConfidence list<float>,
            PRIMARY KEY (SongNumber)
        );
        """
    )

    # The query for inserting a new row in the table
    prepared_query = session.prepare(ADD_ROW_PREPARED)

    # Perform the insertions

    for root, dirs, files in os.walk(base_dir):
        for fs in files:
            if fs.endswith(ext):
                path = os.path.join(root, fs)
                print(path)

                col_vals_dict = dict()
                key_list = None

                with open(path, 'r') as f:

                    for i, line in enumerate(f):
                        # Remove the '\n'
                        line = line[:-1]
                        if i == 0:
                            key_list = [x.strip() for x in line.split(sep)]
                        else:
                            for j, term in enumerate(line.split(sep)):
                                col_vals_dict[key_list[j]] = term

                            col_vals_dict['artistTerms'] = from_string_to_list(col_vals_dict['artistTerms'])
                            col_vals_dict['artistTermsFreq'] = from_string_to_list(col_vals_dict['artistTermsFreq'])
                            col_vals_dict['artistTermsWeight'] = from_string_to_list(col_vals_dict['artistTermsWeight'])
                            col_vals_dict['artistMBTags'] = from_string_to_list(col_vals_dict['artistMBTags'])
                            col_vals_dict['artistMBTagsCount'] = from_string_to_list(col_vals_dict['artistMBTagsCount'])
                            col_vals_dict['similarartists'] = from_string_to_list(col_vals_dict['similarartists'])
                            col_vals_dict['segmentsStart'] = from_string_to_list(col_vals_dict['segmentsStart'])
                            col_vals_dict['segmentsConfidence'] = from_string_to_list(col_vals_dict['segmentsConfidence'])
                            col_vals_dict['segmentsPitches'] = parse_nested_list(from_string_to_list(col_vals_dict['segmentsPitches']))
                            col_vals_dict['segmentsTimbre'] = parse_nested_list(from_string_to_list(col_vals_dict['segmentsTimbre']))
                            col_vals_dict['segmentsLoudnessMax'] = from_string_to_list(col_vals_dict['segmentsLoudnessMax'])
                            col_vals_dict['segmentsLoudnessMaxTime'] = from_string_to_list(col_vals_dict['segmentsLoudnessMaxTime'])
                            col_vals_dict['segmentsLoudnessStart'] = from_string_to_list(col_vals_dict['segmentsLoudnessStart'])
                            col_vals_dict['sectionStarts'] = from_string_to_list(col_vals_dict['sectionStarts'])
                            col_vals_dict['sectionsConfidence'] = from_string_to_list(col_vals_dict['sectionsConfidence'])
                            col_vals_dict['beatsStart'] = from_string_to_list(col_vals_dict['beatsStart'])
                            col_vals_dict['beatsConfidence'] = from_string_to_list(col_vals_dict['beatsConfidence'])
                            col_vals_dict['barsStart'] = from_string_to_list(col_vals_dict['barsStart'])
                            col_vals_dict['barsConfidence'] = from_string_to_list(col_vals_dict['barsConfidence'])
                            col_vals_dict['tatumsStart'] = from_string_to_list(col_vals_dict['tatumsStart'])
                            col_vals_dict['tatumsConfidence'] = from_string_to_list(col_vals_dict['tatumsConfidence'])

                            # print col_vals_dict

                            if col_vals_dict['artistmbid'] == '':
                                col_vals_dict['artistmbid'] = '00000000-0000-0000-0000-000000000000'

                            try:
                                session.execute(
                                    prepared_query .bind((
                                        long(col_vals_dict['SongNumber']),
                                        col_vals_dict['SongID'],
                                        col_vals_dict['albumName'],
                                        long(col_vals_dict['albumID']),
                                        col_vals_dict['artistID'],
                                        float(col_vals_dict['artistLatitude']),
                                        col_vals_dict['artistLocation'],
                                        float(col_vals_dict['artistLongitude']),
                                        float(col_vals_dict['artistFamiliarity']),
                                        float(col_vals_dict['artistHotttnesss']),
                                        uuid.UUID(col_vals_dict['artistmbid']),
                                        long(col_vals_dict['artistPlaymeid']),
                                        long(col_vals_dict['artist7digitalid']),
                                        col_vals_dict['artistTerms'],
                                        float(col_vals_dict['artistTermsCount']),
                                        col_vals_dict['artistTermsFreq'],
                                        col_vals_dict['artistTermsWeight'],
                                        col_vals_dict['artistMBTags'],
                                        int(col_vals_dict['artistMBTagsOuterCount']),
                                        col_vals_dict['artistMBTagsCount'],
                                        int(col_vals_dict['analysisSampleRate']),
                                        col_vals_dict['audioMD5'],
                                        float(col_vals_dict['endOfFadeIn']),
                                        float(col_vals_dict['startOfFadeOut']),
                                        float(col_vals_dict['energy']),
                                        col_vals_dict['release'],
                                        long(col_vals_dict['release7digitalid']),
                                        float(col_vals_dict['songHotness']),
                                        long(col_vals_dict['track7digitalid']),
                                        col_vals_dict['similarartists'],
                                        int(col_vals_dict['similarArtistsCount']),
                                        float(col_vals_dict['loudness']),
                                        int(col_vals_dict['mode']),
                                        float(col_vals_dict['modeConfidence']),
                                        col_vals_dict['artistName'],
                                        float(col_vals_dict['danceability']),
                                        float(col_vals_dict['duration']),
                                        int(col_vals_dict['keySignature']),
                                        float(col_vals_dict['keySignatureConfidence']),
                                        float(col_vals_dict['tempo']),
                                        int(col_vals_dict['timeSignature']),
                                        float(col_vals_dict['timeSignatureConfidence']),
                                        col_vals_dict['title'],
                                        int(col_vals_dict['year']),
                                        col_vals_dict['trackID'],
                                        col_vals_dict['segmentsStart'],
                                        int(col_vals_dict['segmentsCount']),
                                        col_vals_dict['segmentsConfidence'],
                                        col_vals_dict['segmentsPitches'],
                                        col_vals_dict['segmentsTimbre'],
                                        col_vals_dict['segmentsLoudnessMax'],
                                        col_vals_dict['segmentsLoudnessMaxTime'],
                                        col_vals_dict['segmentsLoudnessStart'],
                                        col_vals_dict['sectionStarts'],
                                        int(col_vals_dict['sectionCount']),
                                        col_vals_dict['sectionsConfidence'],
                                        col_vals_dict['beatsStart'],
                                        int(col_vals_dict['beatsCount']),
                                        col_vals_dict['beatsConfidence'],
                                        col_vals_dict['barsStart'],
                                        int(col_vals_dict['barsCount']),
                                        col_vals_dict['barsConfidence'],
                                        col_vals_dict['tatumsStart'],
                                        int(col_vals_dict['tatumsCount']),
                                        col_vals_dict['tatumsConfidence'],
                                    ))
                                )
                            except:
                                pass
