import json
import boto3
import requests
from elasticsearch import Elasticsearch
import os

with open('../elasticsearch_module/es_config.json') as json_file:
    es_config = json.load(json_file)


class ElasticSearch:

    def __init__(self):
        # self.es_client = Elasticsearch(hosts=[{'host': es_config['es_url']}], timeout=10)
        #
        # try:
        #     self.es_client.indices.create(index=es_config['es_titles_index'], ignore= 400)
        # except:
        #     print('Titles index created')
        #
        # try:
        #     self.es_client.indices.create(index=es_config['es_people_index'], ignore= 400)
        # except:
        #     print('People index created')
        pass


    def create_titles_doc(self, title_id):

        json_doc = {'id': title_id}

        none_values = ['name', 'region', 'language', 'is_original_title', 'format',
                       'promotional_title', 'original_title', 'is_adult', 'start_year',
                       'end_year', 'runtime_minutes', ]

        list_values = ['types', 'genres', 'directors', 'writers', 'seasons', 'episodes',
                       'categories', 'people', 'jobs', 'characters']

        zero_values = ['genres_count', 'directors_count', 'writers_count', 'seasons_count',
                       'average_rating', 'total_votes', 'categories_count', 'people_count',
                       'jobs_count', 'characters_count', 'episodes_count']

        for value in none_values:
            json_doc[value] = None

        for value in list_values:
            json_doc[value] = []

        for value in zero_values:
            json_doc[value] = 0

        self.insert_into_es(json_doc, es_config['es_titles_index'], title_id)

        return

    def create_people_doc(self, person_id):

        json_doc = {'id': person_id}

        none_values = ['name', 'birth_year', 'death_year']

        list_values = ['professions', 'movies_acted', 'genres', 'directors', 'formats']

        zero_values = ['years_lived', 'professions_count', 'movies_acted_count', 'genres_count',
                       'directors_count', 'formats_count', 'adult_movies_count', 'non_adult_movies_count']

        for value in none_values:
            json_doc[value] = None

        for value in list_values:
            json_doc[value] = []

        for value in zero_values:
            json_doc[value] = 0

        self.insert_into_es(json_doc, es_config['es_people_index'], person_id)

        return

    def __update_title_akas(self, es_json, table_json, title_id):

        json_doc = es_json['_source']
        json_doc['name'] = table_json['title']
        json_doc['region'] = table_json['region']
        json_doc['language'] = table_json['language']
        json_doc['types'] = table_json['types'].split(',')
        json_doc['is_original_title'] = table_json['isOriginalTitle']

        self.insert_into_es(json_doc, es_config['es_titles_index'], title_id)

    def __update_title_basics(self, es_json, table_json, title_id):

        json_doc = es_json['_source']
        json_doc['format'] = table_json['titleType']
        json_doc['promotional_title'] = table_json['primaryTitle']
        json_doc['original_title'] = table_json['originalTitle']
        json_doc['is_adult'] = table_json['isAdult']
        json_doc['start_year'] = table_json['startYear']
        json_doc['end_year'] = table_json['endYear'] if table_json['endYear'] != '\\N' else json_doc['start_year']
        json_doc['runtime_minutes'] = table_json['runtimeMinutes']
        json_doc['genres'] = table_json['genres'].split(',')
        json_doc['genres_count'] = len(json_doc['genres'])

        self.insert_into_es(json_doc, es_config['es_titles_index'], title_id)

    def __update_title_crew(self, es_json, table_json, title_id):

        json_doc = es_json['_source']
        json_doc['directors'] = table_json['directors'].split(',')
        json_doc['writers'] = table_json['writers'].split(',')
        json_doc['directors_count'] = len(json_doc['directors'])
        json_doc['writers_count'] = len(json_doc['writers'])

        self.insert_into_es(json_doc, es_config['es_titles_index'], title_id)

    def __update_title_ratings(self, es_json, table_json, title_id):

        json_doc = es_json['_source']
        json_doc['average_rating'] = table_json['averageRating']
        json_doc['total_votes'] = table_json['numVotes']

        self.insert_into_es(json_doc, es_config['es_titles_index'], title_id)

    def __update_title_episode(self, es_json, table_json, title_id):

        json_doc = es_json['_source']

        season_number = table_json['seasonNumber']
        if (season_number not in json_doc['seasons']):
            json_doc['seasons'].append(season_number)
        json_doc['seasons_count'] = len(json_doc['seasons'])

        episode_number = str(season_number) + '-' + str(table_json['episodeNumber'])
        if (episode_number not in json_doc['episodes']):
            json_doc['episodes'].append(episode_number)
        json_doc['episodes_count'] = len(json_doc['episodes'])

        self.insert_into_es(json_doc, es_config['es_titles_index'], title_id)

    def __update_title_principals(self, es_json, table_json, title_id):

        json_doc = es_json['_source']

        category = table_json['category']
        if (category not in json_doc['categories']):
            json_doc['categories'].append(category)
        json_doc['categories_count'] = len(json_doc['categories'])

        person = table_json['nconst']
        if (person not in json_doc['people']):
            json_doc['people'].append(person)
        json_doc['people_count'] = len(json_doc['people'])

        job = table_json['job']
        if (job not in json_doc['jobs']):
            json_doc['jobs'].append(job)
        json_doc['jobs_count'] = len(json_doc['jobs'])

        character = table_json['characters']
        if (character not in json_doc['characters']):
            json_doc['characters'].append(character)
        json_doc['characters_count'] = len(json_doc['characters'])

        self.insert_into_es(json_doc, es_config['es_titles_index'], title_id)

    def __update_name_basics(self, es_json, table_json, person_id):

        json_doc = es_json['_source']
        json_doc['name'] = table_json['primaryName']
        json_doc['birth_year'] = int(table_json['birthYear'])
        json_doc['death_year'] = 2020 if table_json['deathYear'] == '\\N' else int(table_json['deathYear'])
        json_doc['years_lived'] = json_doc['death_year'] - json_doc['birth_year']
        json_doc['professions'] = table_json['primaryProfession'].split(',')
        json_doc['professions_count'] = len(json_doc['professions'])

        for title_id in table_json['knownForTitles'].split(','):

            title_json = self.get_json_from_es(es_config['es_titles_index'], title_id)['_source']
            json_doc['movies_acted'].append(title_id)
            json_doc['movies_acted_count'] += 1
            if (title_json['is_adult'] == 0):
                json_doc['non_adult_movies_count'] += 1
            else:
                json_doc['adult_movies_count'] += 1

            genres = title_json['genres']
            for genre in genres:
                if (genre not in json_doc['genres']):
                    json_doc['genres'].append(genre)
                    json_doc['genres_count'] += 1

            directors = title_json['directors']
            for director in directors:
                if (director not in json_doc['directors']):
                    json_doc['directors'].append(director)
                    json_doc['directors_count'] += 1

            format = title_json['format']
            if (format not in json_doc['formats']):
                json_doc['formats'].append(format)
                json_doc['formats_count'] += 1

        self.insert_into_es(json_doc, es_config['es_people_index'], person_id)

    def insert_into_es(self, json_doc, index, index_id):

        try:
            insert_request = requests.post(url="{}:{}/{}/_doc/{}".format(es_config['es_url'], es_config['es_port']
                                                                         , index, index_id),
                                           data=json.dumps(json_doc),
                                           headers=es_config['es_request_headers']).json()


        except Exception as e:
            exit(0)

    def get_json_from_es(self, index, index_id):

        json_doc = requests.get(url='{}:{}/{}/_doc/{}'.format(es_config['es_url'], es_config['es_port'],
                                                              index, index_id)).json()

        if (not json_doc['found'] and index == es_config['es_titles_index']):
            self.create_titles_doc(index_id)

        elif (not json_doc['found'] and index == es_config['es_people_index']):
            self.create_people_doc(index_id)

        json_doc = requests.get(url='{}:{}/{}/_doc/{}'.format(es_config['es_url'], es_config['es_port'],
                                                              index, index_id)).json()

        return json_doc

    def update_titles_doc(self, table_name, table_json, title_id):

        es_json = self.get_json_from_es(es_config['es_titles_index'], title_id)

        if (table_name == 'title_akas'):
            self.__update_title_akas(es_json, table_json, title_id)
        elif (table_name == 'title_basics'):
            self.__update_title_basics(es_json, table_json, title_id)
        elif (table_name == 'title_crew'):
            self.__update_title_crew(es_json, table_json, title_id)
        elif (table_name == 'title_episode'):
            self.__update_title_episode(es_json, table_json, title_id)
        elif (table_name == 'title_ratings'):
            self.__update_title_ratings(es_json, table_json, title_id)
        elif (table_name == 'title_principals'):
            self.__update_title_principals(es_json, table_json, title_id)

        return

    def update_people_doc(self, table_json, person_id):

        es_json = self.get_json_from_es(es_config['es_people_index'], person_id)
        self.__update_name_basics(es_json, table_json, person_id)
        return

# es = ElasticSearch()
# print(es.get_json_from_es('titles', 'tt000002'))
# print(es.get_json_from_es('people', 'nm000002'))
