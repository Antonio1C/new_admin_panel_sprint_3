from datetime import datetime
from psycopg2.extensions import connection
from status import Status

class PGLoader:

    def __init__(self, conn: connection) -> None:
        self.conn = conn
    
    def __get_query_template(self):
        return '''
        SELECT
            fw.id,
            fw.title,
            fw.description,
            fw.rating,
            fw.type,
            fw.modified,
            COALESCE (
                json_agg(
                    DISTINCT jsonb_build_object(
                        'person_role', pfw.role,
                        'person_id', p.id,
                        'person_name', p.full_name
                    )
                ) FILTER (WHERE p.id is not null),
                '[]'
            ) as persons,
            array_agg(DISTINCT g.name) as genres
        FROM content.film_work AS fw
        LEFT JOIN content.person_film_work AS pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.person AS p ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work AS gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre AS g ON g.id = gfw.genre_id
        WHERE fw.id IN {}
        GROUP BY fw.id
        ORDER BY fw.modified;'''

    def get_movies_from_database(self, mod_date: datetime):
        cur = self.conn.cursor()
        for fw_ids in self.get_changes(mod_date):

            if not fw_ids: break

            sql_query = self.__get_query_template().format(tuple(fw_ids))
            cur.execute(sql_query)
            yield cur.fetchall()
    
        cur.close()
        return None

    def get_changes(self, mod_date: datetime):

        cur = self.conn.cursor()
        sql_query = '''
            SELECT fw.id AS id
            FROM content.film_work fw
            '''
        
        if mod_date is None:
            sql_query += ';'
        else:
            sql_query += f'WHERE fw.modified > {mod_date}'
            sql_query += ('''
                UNION
                SELECT pfw.film_work_id
                FROM content.person_film_work AS pfw
                WHERE pfw.person_id IN (SELECT p.id
                    FROM content.person AS p
                    WHERE p.modified > ''' + f'{mod_date})' + '''
                
                UNION
                SELECT 
                FROM content.genre_film_work AS gfw
                WHERE gfw.genre_id IN (SELECT g.id
                    FROM content.genre AS g
                    WHERE g.modified > ''' + f'{mod_date});')
        
        cur.execute(sql_query)

        fw_ids = []
        while True:
            fw_ids.clear()
            result = cur.fetchmany(500)

            if not result:
                cur.close()
                return []
            
            for row in result:
                fw_ids.append(row['id'])
            
            yield fw_ids

        



