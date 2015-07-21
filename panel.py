import urllib.request
import csv
import codecs
import sqlite3
import itertools
from collections import Counter

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def tieBreaker(results, key) :
    for key, rows in itertools.groupby(results, key) :
        seen_ids = set()
        for row in rows :
            left_id, right_id = row['left_id'], row['right_id']
            if left_id not in seen_ids and right_id not in seen_ids :
                yield row
                seen_ids.add(left_id)
                seen_ids.add(right_id)

def insertSnapshot(time, four_by_four) :
    cur.execute("CREATE TABLE %s (name, position, salary, id INTEGER PRIMARY KEY)" % time)

    url = "https://data.cityofchicago.org/resource/%s.csv?department=POLICE&$limit=50000" % four_by_four

    try :
        response = urllib.request.urlopen(url)
    except :
        print(url)
        raise

    reader = csv.reader(codecs.getreader('utf-8')(response))
    next(reader)

    cur.executemany("INSERT INTO %s (name, position, salary) VALUES (?, ?, ?)" % time,
                    ((row[0].strip(), 
                      row[1].strip(), 
                      row[3].strip()) for row in reader))

    con.commit()

def merge(tables, keys, additional_condition='') :

    a, b = tables

    key_string = ', '.join(keys)

    unique_sql = """
    INSERT INTO {0}{1} (name, {0}_id, {1}_id) 
      SELECT name, left.id AS left_id, right.id AS right_id 
      FROM {0} AS left INNER JOIN {1} AS right
      USING ({2}) 
      WHERE left.id NOT IN 
          (SELECT {0}_id FROM {0}{1} WHERE {0}_id IS NOT NULL)
        AND
        right.id NOT IN 
          (SELECT {1}_id FROM {0}{1} WHERE {1}_id IS NOT NULL)
      {3} 
      GROUP BY {2} HAVING COUNT(*) < 2
    """

    cur.execute(unique_sql.format(a, b, key_string, additional_condition))
    con.commit()

    cartesian_sql = """
    SELECT {2}, left.id AS left_id, right.id AS right_id 
    FROM {0} AS left INNER JOIN {1} AS right
    USING ({2}) 
    WHERE left.id NOT IN 
          (SELECT {0}_id FROM {0}{1} WHERE {0}_id IS NOT NULL)
        AND
        right.id NOT IN 
          (SELECT {1}_id FROM {0}{1} WHERE {1}_id IS NOT NULL)
    {3}
    ORDER BY {2}
    """

    cartesian = list(cur.execute(cartesian_sql.format(a, b,
                                                      key_string, 
                                                      additional_condition)))

    for row in tieBreaker(cartesian, lambda x: tuple(x[key] for key in keys)) :
        cur.execute("INSERT INTO {0}{1} (name, {0}_id, {1}_id) VALUES (?, ?, ?)".format(a, b),
                    (row['name'], 
                     row['left_id'], 
                     row['right_id']))
        con.commit()

def joinTables(table_pair) :
    higher_salary = "AND CAST(LTRIM(left.salary, '$') AS DECIMAL) < CAST(LTRIM(right.salary, '$') AS DECIMAL)"

    merge(table_pair, ['name', 'position', 'salary'])

    merge(table_pair, ['name', 'position'], higher_salary)
    merge(table_pair, ['name'], higher_salary)
    merge(table_pair, ['name'])

    a, b = table_pair

    for table in table_pair :
        cur.execute("INSERT INTO {0}{1} (name, {2}_id) SELECT name, id FROM {2} WHERE id NOT IN (SELECT {2}_id FROM {0}{1} WHERE {2}_id IS NOT NULL)".format(a, b, table))
        con.commit()

def spanTable(snapshots) :

    tables = ['t' + str(i) for i in range(len(snapshots))]

    for table, snapshot in zip(tables, snapshots) :
        date, four_by_four = snapshot
        insertSnapshot(table, four_by_four)

    for table_pair in zip(tables[:-1], tables[1:]) :
        cur.execute("CREATE TABLE {0}{1} (name, {0}_id, {1}_id)".format(*table_pair))
        joinTables(table_pair)



con = sqlite3.connect("foo")
con.row_factory = dict_factory

cur = con.cursor()

snapshots = [('2015-06-01', '8a9j-uaug'),
             ('2014-06-09', '8c7s-25ji'),
             ('2014-01-13', 'hcfc-b6bn'),
             ('2013-10-03', 'ika2-ij72'),
             ('2013-09-26', '4pid-t6mp'),
             ('2013-04-24', 'xgyw-pv8f'),
             #('2013-02-18', 'nzdg-pgzr'),
             ('2012-10-08', 'vkgi-mmv8'),
             ('2012-08-31', 'vpki-baq8'),
             ('2012-04-19', 'rumj-qya8')]
snapshots.sort()

spanTable(snapshots)

table_names = ['t' + str(i) for i in range(len(snapshots))]

for i, (a, b, c) in enumerate(zip(table_names[:-2], 
                                  table_names[1:-1], 
                                  table_names[2:])) :
    if i == 0 :
        sub_query = """
         SELECT t0t1.*, t1t2.t2_id
         FROM t0t1 LEFT OUTER JOIN t1t2 
         USING (t1_id) 
         UNION 
         SELECT t0t1.*, t1t2.t2_id 
         FROM t1t2 LEFT OUTER JOIN t0t1
         USING (t1_id)
         WHERE t0t1.t0_id IS NULL
         """
        alias = a + b + c
        
    else :
        sub_query = """
         SELECT {alias}.*, {b}{c}.{c}_id
         FROM ({sub_query}) AS {alias} LEFT OUTER JOIN {b}{c}
         USING ({b}_id)
         UNION
         SELECT {alias}.*, {b}{c}.{c}_id
         FROM {b}{c} LEFT OUTER JOIN ({sub_query}) AS {alias}
         USING ({b}_id) 
         WHERE {alias}.{a}_id IS NULL
        """.format(alias = alias,
                   sub_query = sub_query,
                   a = a,
                   b = b,
                   c = c)
        alias += c

columns = [table + '_id' for table in table_names]
column_string = ', '.join(columns)

cur.execute('CREATE TABLE span (id INTEGER PRIMARY KEY, name, {0})'.format(column_string))

con.commit()

cur.execute('INSERT INTO span (name, {0}) {1}'.format(column_string,
                                                      sub_query))

con.commit()

cur.execute('CREATE TABLE jobs (id INTEGER PRIMARY KEY, person_id, name, position, begin, end)')
con.commit()

for row in list(cur.execute("SELECT * FROM span")) :

    yearly_ids = [row[col] for col in sorted(row) if col.endswith('_id')]
    observations = [cur.execute("SELECT name, position from t{0} where id = ?".format(i), (person_id,)).fetchone() if person_id else None for i, person_id in enumerate(yearly_ids)]

    current_position = None
    position_spans = {}
    current_name = None


    for i, observation in enumerate(observations) :
        if observation :
            name = observation['name']
            if name :
                current_name = name
            position = observation['position']
        else :
            position = None
        if position  :
            if position != current_position :
                position_spans[position] = [i, i]
                if current_position :
                    position_spans[current_position][1] = i
                current_position = position
            else :
                position_spans[position][1] = i
    
    cur.executemany("INSERT INTO jobs (person_id, name, position, begin, end) VALUES (?, ?, ?, ?, ?)", 
                    ((row['id'],
                      current_name,
                      position, 
                      snapshots[duration[0]][0], 
                      snapshots[duration[1]][0]) 
                     for position, duration in position_spans.items()))

con.commit()

with open('position_span.csv', 'w') as outfile :
    writer = csv.writer(outfile)
    writer.writerow(['id', 'name', 'position', 'begin', 'end'])

    for row in cur.execute("SELECT * FROM jobs") :
        writer.writerow([row['person_id'], row['name'], row['position'], 
                         row['begin'], row['end']])
