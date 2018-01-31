# Copyright 2018 Cisco Systems, Inc.
# All rights reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import json

from neo4j.v1 import GraphDatabase

driver = GraphDatabase.driver("bolt://10.75.44.192:7687", auth=("neo4j", "cisco123"))
session = driver.session()
f = open('1517386971.52.msg')


def get_path(originator_id, cluster_list):
    """link list
    """
    path_list = []
    cluster_list.reverse()
    path_list.append((originator_id, cluster_list[0]))
    i = 0
    while i < len(cluster_list) - 1:
        path_list.append((cluster_list[i], cluster_list[i + 1]))
        i += 1
    return path_list


sql = """MERGE (c:%s {ip: "%s"})
MERGE (r:%s {ip: "%s"})
MERGE (r)-[:CONNECT]->(c)
"""

with driver.session() as session:
    for line in f:
        line = json.loads(line)
        if line.get('type') != 2:
            continue
        attr = line['msg'].get('attr') or {}
        origin = attr.get('9')
        cluster = attr.get('10')
        if not origin or not cluster:
            continue
        
        path_list = get_path(origin, cluster)
        print path_list
        session.run(sql % ('Client', path_list[0][0], 'RR', path_list[0][1]))
        for path in path_list[1:]:
            session.run(sql % ('RR', path[0], 'RR', path[1]))


"""
MATCH (n) RETURN n LIMIT 150
MATCH (r1:Client {ip:"219.158.1.197"}), (r2:Client {ip:"219.158.1.209"}),
      path = shortestpath((r1)-[:CONNECT*]-(r2))
RETURN path
"""