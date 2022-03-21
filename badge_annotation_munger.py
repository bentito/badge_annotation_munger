# Read the CSV blob and change the subscription badge annotation as needed.

# Source of truth: https://docs.openshift.com/container-platform/4.9/welcome/oke_about.html#feature-summary
# usage: badge_annotation_munger.py <catalog_index_to_update>
import sqlite3
import sys
from sqlite3 import Error
import yaml
import csv


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)
    return conn


def get_source_of_truth():
    """*** Note, Note, Note: Do NOT take this included CSV as really a source of truth, for prototype ONLY ***"""
    with open('oke_subs.csv', newline='') as csvfile:
        oke_subs = list(csv.DictReader(csvfile, delimiter=','))
        return oke_subs


def decode_subscription_to_int(subcription_string):
    """ see if it's included or not in a given subscription entry
    :param db_file: database file
    :return: return 0 for not included, 1 for included, and 2 for all else
    """
    if subcription_string.lower().startswith("not"):
        return 0
    if subcription_string.lower().startswith("included"):
        return 1
    # TODO verify this is acceptable, if we ever get anything but included or not?
    return 0


if __name__ == '__main__':
    index = sys.argv[1]  # pass the index file (sqlite db) to work on, 4.9-rh, etc.
    conn = create_connection(index)
    cur = conn.cursor()
    cur.execute("SELECT t.name, t.csv FROM operatorbundle t;")
    operator_bundles = cur.fetchall()  # has all the bundles

    subscription_truth = get_source_of_truth()  # has all the bundles names we have subscription info for
    for subscription_truth_line in subscription_truth:
        useful_operator_name = subscription_truth_line['Useful_Operator_Name']
        oke_subscription = subscription_truth_line['OpenShift_Kubernetes_Engine']
        ocp_subscription = subscription_truth_line['OpenShift_Container_Platform']
        opp_subscription = subscription_truth_line['OpenShift_Platform_Plus']

        print("working on operator: %s" % useful_operator_name)
        for operator_bundle in operator_bundles:
            bundle_name = operator_bundle[0]
            if bundle_name.startswith(useful_operator_name):
                csv = operator_bundle[1].decode()
                csv_yaml = yaml.safe_load(csv)  # get the as-is yaml blob
                print("  working on bundle: %s" % bundle_name)

                # logic for building the subscription entry based on subs needs for a given operator
                subscription_entry = """'["""
                if decode_subscription_to_int(oke_subscription) == 1:
                    subscription_entry += '"OpenShift Kubernetes Engine", '
                if decode_subscription_to_int(ocp_subscription) == 1:
                    subscription_entry += '"OpenShift Container Platform", '
                if decode_subscription_to_int(opp_subscription) == 1:
                    subscription_entry += '"OpenShift Platform Plus", '
                subscription_entry = subscription_entry[:-2]
                subscription_entry += """]'"""

                # TODO check this is right!:
                # if this operator isn't included with any subscription, it must be its own subscription
                if decode_subscription_to_int(oke_subscription) + decode_subscription_to_int(ocp_subscription) + \
                        decode_subscription_to_int(opp_subscription) == 0:
                    subscription_entry = subscription_entry = """'[""" + '"' + useful_operator_name + '"' + """]'"""

                # add the needed subscription entries in annotations
                # csv_yaml['metadata']['annotations'][
                #     'operators.openshift.io/valid-subscription'] = '["OpenShift Container Platform"]'
                csv_yaml['metadata']['annotations'][
                    'operators.openshift.io/valid-subscription'] = subscription_entry

                # re-encode to bytes
                csv_yaml_bytes = yaml.dump(csv_yaml).encode()
                # update the csv blob
                cur.execute("UPDATE operatorbundle SET csv=? WHERE name=?", (csv_yaml_bytes, bundle_name))
                print("    number of rows updated: %d" % cur.rowcount)
                conn.commit()
                pass
    conn.close()
