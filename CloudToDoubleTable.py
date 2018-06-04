import StringIO
import requests
import sys
import uuid
from zipfile import ZipFile
import numpy as np
from cassandra.cluster import Cluster
import os


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
    archive_ext = ".zip"  # Set the extension of the archive file

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

    ADD_ROW_METADATA_PREPARED = """
           INSERT INTO million_song.song_metadata (
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
               artistTermsCount,
               artistMBTagsOuterCount,
               analysisSampleRate,
               audioMD5,
               endOfFadeIn,
               startOfFadeOut,
               energy,
               release,
               release7digitalid,
               songHotness,
               track7digitalid,
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
               segmentsCount,
               sectionCount,
               beatsCount,
               barsCount,
               tatumsCount
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
               ?            
           )
       """

    ADD_ROW_LISTS_PREPARED = """
           INSERT INTO million_song.song_lists (
               SongNumber,
               artistTerms,
               artistTermsFreq,
               artistTermsWeight,
               artistMBTags,
               artistMBTagsCount,
               similarartists,
               segmentsStart,
               segmentsConfidence,
               segmentsPitches,
               segmentsTimbre,
               segmentsLoudnessMax,
               segmentsLoudnessMaxTime,
               segmentsLoudnessStart,
               sectionStarts,
               sectionsConfidence,
               beatsStart,
               beatsConfidence,
               barsStart,
               barsConfidence,
               tatumsStart,
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

    # The metadata table
    session.execute(
        """
        DROP TABLE IF EXISTS million_song.song_metadata
        """
    )

    # The lists table
    session.execute(
        """
        DROP TABLE IF EXISTS million_song.song_lists
        """
    )

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS million_song.song_metadata (
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
            artistTermsCount float,
            artistMBTagsOuterCount int,
            analysisSampleRate int,
            audioMD5 text,
            endOfFadeIn float,
            startOfFadeOut float,
            energy float,
            release text,
            release7digitalid bigint,
            songHotness double,
            track7digitalid bigint,
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
            segmentsCount int,
            sectionCount int,
            beatsCount int,
            barsCount int,
            tatumsCount int,
            PRIMARY KEY (SongNumber)
        );
        """
    )

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS million_song.song_lists (
            SongNumber bigint,
            artistTerms list<text>,
            artistTermsFreq list<double>,
            artistTermsWeight list<double>,
            artistMBTags list<text>,
            artistMBTagsCount list<float>,
            similarartists list<text>,
            segmentsStart list<float>,
            segmentsConfidence list<float>,
            segmentsPitches list<frozen <list<float>>>,
            segmentsTimbre list<frozen <list<float>>>,
            segmentsLoudnessMax list<float>,
            segmentsLoudnessMaxTime list<float>,
            segmentsLoudnessStart list<float>,
            sectionStarts list<float>,
            sectionsConfidence list<float>,
            beatsStart list<float>,
            beatsConfidence list<float>,
            barsStart list<float>,
            barsConfidence list<float>,
            tatumsStart list<float>,
            tatumsConfidence list<float>,
            PRIMARY KEY (SongNumber)
        );
        """
    )

    # The query for inserting a new row in the table
    prepared_query_metadata = session.prepare(ADD_ROW_METADATA_PREPARED)
    prepared_query_lists = session.prepare(ADD_ROW_LISTS_PREPARED)

    # Perform the insertions

    csv_dump_path = os.path.join(base_dir, "temp")
    url_sources = [
        'https://s3-us-west-2.amazonaws.com/wdm-oregon-bucket/A.zip'
        'https://s3-us-west-2.amazonaws.com/wdm-oregon-bucket/B.zip',
        'https://s3-us-west-2.amazonaws.com/wdm-oregon-bucket/C.zip',
        'https://s3-us-west-2.amazonaws.com/wdm-oregon-bucket/D.zip',
        'https://s3-us-west-2.amazonaws.com/wdm-oregon-bucket/E.zip',
        'https://s3-us-west-2.amazonaws.com/wdm-oregon-bucket/F.zip',
        'https://s3-us-west-2.amazonaws.com/wdm-oregon-bucket/G.zip',
        'https://s3-us-west-2.amazonaws.com/wdm-oregon-bucket/H.zip',
        'https://s3-us-west-2.amazonaws.com/wdm-oregon-bucket/I.zip',
        'https://s3-us-west-2.amazonaws.com/wdm-oregon-bucket/J.zip',
        'https://s3-us-west-2.amazonaws.com/wdm-oregon-bucket/K.zip',
        'https://s3-us-west-2.amazonaws.com/wdm-oregon-bucket/L.zip',
        'https://s3-us-west-2.amazonaws.com/wdm-oregon-bucket/M.zip',
        'https://s3-us-west-2.amazonaws.com/wdm-oregon-bucket/N.zip',
        'https://s3-us-west-2.amazonaws.com/wdm-oregon-bucket/O.zip',
        'https://s3-us-west-2.amazonaws.com/wdm-oregon-bucket/P.zip',
        'https://s3-us-west-2.amazonaws.com/wdm-oregon-bucket/Q.zip',
        'https://s3-us-west-2.amazonaws.com/wdm-oregon-bucket/R.zip',
        'https://s3-us-west-2.amazonaws.com/wdm-oregon-bucket/S.zip',
        'https://s3-us-west-2.amazonaws.com/wdm-oregon-bucket/T.zip',
        'https://s3-us-west-2.amazonaws.com/wdm-oregon-bucket/U.zip',
        'https://s3-us-west-2.amazonaws.com/wdm-oregon-bucket/V.zip',
        'https://s3-us-west-2.amazonaws.com/wdm-oregon-bucket/W.zip',
        'https://s3-us-west-2.amazonaws.com/wdm-oregon-bucket/X.zip',
        'https://s3-us-west-2.amazonaws.com/wdm-oregon-bucket/Y.zip',
        'https://s3-us-west-2.amazonaws.com/wdm-oregon-bucket/Z.zip',
    ]

    for source in url_sources:
        print "Downloading ", source
        r = requests.get(source, stream=True)
        with ZipFile(StringIO.StringIO(r.content)) as zipArchive:
            print "Finished Downloading", source

            print zipArchive.namelist()

            for csv_path in [x for x in zipArchive.namelist() if ext in x]:
                absolute_csv_path = os.path.join(csv_dump_path, csv_path)
                zipArchive.extract(csv_path, csv_dump_path)

                col_vals_dict = dict()
                key_list = None

                with open(absolute_csv_path, 'r') as f:

                    for i, line in enumerate(f):
                        # Remove the '\n'
                        line = line[:-1]
                        if i == 0:
                            key_list = [x.strip() for x in line.split(sep)]
                        else:
                            for j, term in enumerate(line.split(sep)):
                                col_vals_dict[key_list[j]] = term

                            col_vals_dict['artistTerms'] = from_string_to_list(col_vals_dict['artistTerms'])
                            col_vals_dict['artistTermsFreq'] = from_string_to_list(
                                col_vals_dict['artistTermsFreq'])
                            col_vals_dict['artistTermsWeight'] = from_string_to_list(
                                col_vals_dict['artistTermsWeight'])
                            col_vals_dict['artistMBTags'] = from_string_to_list(col_vals_dict['artistMBTags'])
                            col_vals_dict['artistMBTagsCount'] = from_string_to_list(
                                col_vals_dict['artistMBTagsCount'])
                            col_vals_dict['similarartists'] = from_string_to_list(
                                col_vals_dict['similarartists'])
                            col_vals_dict['segmentsStart'] = from_string_to_list(col_vals_dict['segmentsStart'])
                            col_vals_dict['segmentsConfidence'] = from_string_to_list(
                                col_vals_dict['segmentsConfidence'])
                            col_vals_dict['segmentsPitches'] = parse_nested_list(
                                from_string_to_list(col_vals_dict['segmentsPitches']))
                            col_vals_dict['segmentsTimbre'] = parse_nested_list(
                                from_string_to_list(col_vals_dict['segmentsTimbre']))
                            col_vals_dict['segmentsLoudnessMax'] = from_string_to_list(
                                col_vals_dict['segmentsLoudnessMax'])
                            col_vals_dict['segmentsLoudnessMaxTime'] = from_string_to_list(
                                col_vals_dict['segmentsLoudnessMaxTime'])
                            col_vals_dict['segmentsLoudnessStart'] = from_string_to_list(
                                col_vals_dict['segmentsLoudnessStart'])
                            col_vals_dict['sectionStarts'] = from_string_to_list(col_vals_dict['sectionStarts'])
                            col_vals_dict['sectionsConfidence'] = from_string_to_list(
                                col_vals_dict['sectionsConfidence'])
                            col_vals_dict['beatsStart'] = from_string_to_list(col_vals_dict['beatsStart'])
                            col_vals_dict['beatsConfidence'] = from_string_to_list(
                                col_vals_dict['beatsConfidence'])
                            col_vals_dict['barsStart'] = from_string_to_list(col_vals_dict['barsStart'])
                            col_vals_dict['barsConfidence'] = from_string_to_list(
                                col_vals_dict['barsConfidence'])
                            col_vals_dict['tatumsStart'] = from_string_to_list(col_vals_dict['tatumsStart'])
                            col_vals_dict['tatumsConfidence'] = from_string_to_list(
                                col_vals_dict['tatumsConfidence'])

                            # print col_vals_dict

                            if col_vals_dict['artistmbid'] == '':
                                col_vals_dict['artistmbid'] = '00000000-0000-0000-0000-000000000000'

                            try:
                                session.execute(
                                    prepared_query_metadata.bind((
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
                                        float(col_vals_dict['artistTermsCount']),
                                        int(col_vals_dict['artistMBTagsOuterCount']),
                                        int(col_vals_dict['analysisSampleRate']),
                                        col_vals_dict['audioMD5'],
                                        float(col_vals_dict['endOfFadeIn']),
                                        float(col_vals_dict['startOfFadeOut']),
                                        float(col_vals_dict['energy']),
                                        col_vals_dict['release'],
                                        long(col_vals_dict['release7digitalid']),
                                        float(col_vals_dict['songHotness']),
                                        long(col_vals_dict['track7digitalid']),
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
                                        int(col_vals_dict['segmentsCount']),
                                        int(col_vals_dict['sectionCount']),
                                        int(col_vals_dict['beatsCount']),
                                        int(col_vals_dict['barsCount']),
                                        int(col_vals_dict['tatumsCount'])
                                    ))
                                )

                                session.execute(
                                    prepared_query_lists.bind((
                                        long(col_vals_dict['SongNumber']),
                                        col_vals_dict['artistTerms'],
                                        col_vals_dict['artistTermsFreq'],
                                        col_vals_dict['artistTermsWeight'],
                                        col_vals_dict['artistMBTags'],
                                        col_vals_dict['artistMBTagsCount'],
                                        col_vals_dict['similarartists'],
                                        col_vals_dict['segmentsStart'],
                                        col_vals_dict['segmentsConfidence'],
                                        col_vals_dict['segmentsPitches'],
                                        col_vals_dict['segmentsTimbre'],
                                        col_vals_dict['segmentsLoudnessMax'],
                                        col_vals_dict['segmentsLoudnessMaxTime'],
                                        col_vals_dict['segmentsLoudnessStart'],
                                        col_vals_dict['sectionStarts'],
                                        col_vals_dict['sectionsConfidence'],
                                        col_vals_dict['beatsStart'],
                                        col_vals_dict['beatsConfidence'],
                                        col_vals_dict['barsStart'],
                                        col_vals_dict['barsConfidence'],
                                        col_vals_dict['tatumsStart'],
                                        col_vals_dict['tatumsConfidence']
                                    ))
                                )
                            except Exception as err:
                                print err
