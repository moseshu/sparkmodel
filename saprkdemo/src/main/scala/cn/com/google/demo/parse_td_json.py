import json
import sys


def process_frequency_one_dim(prefix, conditions, line_res):
    for condition in conditions:
        key = "%s-%s" % (prefix, condition['dim_type'])
        value = condition['result']
        line_res[key] = value


def process_frequency_count(prefix, conditions, line_res):
    for condition in conditions:
        key = "%s-%s-%s" % (prefix, condition['dim_type'], condition['sub_dim_type'])
        value = condition['result']
        line_res[key] = value


def process_frequency_count(prefix, conditions, line_res):
    for condition in conditions:
        key = "%s-%s-%s" % (prefix, condition['dim_type'], condition['sub_dim_type'])
        value = condition['result']
        line_res[key] = value


def process_frequency_distinct(prefix, conditions, line_res):
    process_frequency_count(prefix, conditions, line_res)


def process_black_list(prefix, conditions, line_res):
    for condition in conditions:
        for hit in condition['hits']:
            key = "%s-%s-%s" % (prefix, condition['dim_type'], hit['fraud_type_display_name'])
            if key in line_res:
                line_res[key] += 1
            else:
                line_res[key] = 1


def process_fp_exception(prefix, conditions, line_res):
    for condition in conditions:
        key = "%s-%s" % (prefix, condition['code_display_name'])
        if key in line_res:
            line_res[key] += 1
        else:
            line_res[key] = 1


def process_grey_list(prefix, conditions, line_res):
    process_black_list(prefix, conditions, line_res)


def process_fuzzy_black_list(prefix, conditions, line_res):
    process_black_list(prefix, conditions, line_res)


def process_geo_ip_distance(prefix, conditions, line_res):
    for condition in conditions:
        key = "%s-%s" % (prefix, condition['dim_type'])
        value = condition['result']
        if condition['unit'] == 'm':
            value = round(float(value) / 1000, 2)
        line_res[key] = value


def process_proxy_ip(prefix, conditions, line_res):
    for condition in conditions:
        key = "%s-%s" % (prefix, condition['proxy_ip_type'])
        if key in line_res:
            line_res[key] += 1
        else:
            line_res[key] = 1


def process_match_address(prefix, conditions, line_res):
    key = prefix
    value = len(conditions)
    line_res[key] = str(value)


def process_gps_distance(prefix, conditions, line_res):
    for condition in conditions:
        key = "%s-%s-%s" % (prefix, condition['gps_a'], condition['gps_b'])
        value = condition['result']
        line_res[key] = value


def process_regex(prefix, conditions, line_res):
    for condition in conditions:
        key = "%s-%s" % (prefix, condition['dim_type'])
        value = '1'
        line_res[key] = value


def process_event_time_diff(prefix, conditions, line_res):
    for condition in conditions:
        key = prefix
        value = condition['result']
        line_res[key] = value


def process_time_diff(prefix, conditions, line_res):
    process_event_time_diff(prefix, conditions, line_res)


def process_active_days_two(prefix, conditions, line_res):
    for condition in conditions:
        key = "%s-%s-%s" % (prefix, condition['dim_type'], condition['sub_dim_type'])
        value = condition['result']
        line_res[key] = value


def process_cross_event(prefix, conditions, line_res):
    for condition in conditions:
        key = "%s-%s-%s" % (prefix, condition['dim_type'], condition['sub_dim_type'])
        value = condition['result']
        line_res[key] = value


def process_cross_velocity_one_dim(prefix, conditions, line_res):
    for condition in conditions:
        key = "%s-%s-%s" % (prefix, condition['dim_type'], condition['match_dim_type'])
        value = condition['result']
        line_res[key] = value


def process_cross_velocity_count(prefix, conditions, line_res):
    for condition in conditions:
        key = "%s-%s-%s-%s" % (prefix, condition['dim_type'], condition['sub_dim_type'], condition['match_dim_type'])
        value = condition['result']
        line_res[key] = value


def process_cross_velocity_distinct(prefix, conditions, line_res):
    for condition in conditions:
        key = "%s-%s-%s-%s" % (prefix, condition['dim_type'], condition['sub_dim_type'], condition['match_dim_type'])
        value = condition['result']
        line_res[key] = value


def process_calculate(prefix, conditions, line_res):
    for condition in conditions:
        key = "%s-%s-%s-%s" % (prefix, condition['dim_type'], condition['sub_dim_type'], condition['calc_type'])
        value = condition['result']
        line_res[key] = value


def process_last_match(prefix, conditions, line_res):
    for condition in conditions:
        key = "%s-%s" % (prefix, condition['dim_type'])
        value = condition['result']
        line_res[key] = value


def process_min_max(prefix, conditions, line_res):
    for condition in conditions:
        key = "%s-%s-%s-%s" % (prefix, condition['dim_type'], condition['sub_dim_type'], condition['calc_type'])
        value = condition['result']
        line_res[key] = value


def process_count(prefix, conditions, line_res):
    for condition in conditions:
        key = "%s-%s-%s" % (prefix, condition['dim_type'], condition['sub_dim_type'])
        value = condition['result']
        line_res[key] = value


def process_association_partner(prefix, conditions, line_res):
    for condition in conditions:
        key = prefix
        value = condition['result']
        line_res[key] = value
        for hit in condition['hits']:
            key = "%s-%s" % (prefix, hit['industry_display_name'])
            line_res[key] = hit['count']
        for hit_for_dim in condition['hits_for_dim']:
            key = "%s-%s-%s" % (prefix, hit_for_dim['dim_type'], hit_for_dim['industry_display_name'])
            value = hit_for_dim['count']
            line_res[key] = value
        for result_for_dim in condition['results_for_dim']:
            key = key = "%s-%s" % (prefix, result_for_dim['dim_type'])
            value = result_for_dim['count']
            line_res[key] = value


def process_discredit_count(prefix, conditions, line_res):
    for condition in conditions:
        key = "%s-%s-%s" % (prefix, condition['calc_dim_type'], condition['calc_type'])
        value = condition['result']
        line_res[key] = value
        # print condition
        for hit in condition['hits']:
            # print hit
            if 'overdue_amount' in hit:
                key = "%s-%s-%s-%s" % (
                prefix, condition['calc_dim_type'], condition['calc_type'], hit['overdue_amount'])
            else:
                key = "%s-%s-%s-%s" % (prefix, condition['calc_dim_type'], condition['calc_type'], str(-1))
            value = 1
            if key in line_res:
                line_res[key] += 1
            else:
                line_res[key] = value


def process_cross_partner(prefix, conditions, line_res):
    for condition in conditions:
        key = "%s-%s" % (prefix, condition['event_type'])
        value = condition['result']
        line_res[key] = value
        for hit in condition['hits']:
            key = "%s-%s" % (prefix, hit['industry_display_name'])
            line_res[key] = hit['count']
        for hit_for_dim in condition['hits_for_dim']:
            key = "%s-%s-%s-%s" % (prefix, hit_for_dim['original_dim_type'], \
                                   hit_for_dim['match_dim_type'], hit_for_dim['industry_display_name'])
            value = hit_for_dim['count']
            line_res[key] = value
        for result_for_dim in condition['results_for_dim']:
            key = "%s-%s-%s" % (prefix, hit_for_dim['original_dim_type'], \
                                hit_for_dim['match_dim_type'])
            value = result_for_dim['count']
            line_res[key] = value


def process_four_calculation(prefix, conditions, line_res):
    for condition in conditions:
        key = prefix
        value = condition['result']
        line_res[key] = value


def process_function_kit(prefix, conditions, line_res):
    for condition in conditions:
        key = prefix
        value = condition['result']
        line_res[key] = value


def process_usual_browser(prefix, conditions, line_res):
    for condition in conditions:
        key = "%s-%s-%s" % (prefix, condition['dim_type'], \
                            condition['unit'])
        value = condition['result']
        line_res[key] = value


def process_usual_device(prefix, conditions, line_res):
    for condition in conditions:
        key = "%s-%s-%s" % (prefix, condition['dim_type'], \
                            condition['unit'])
        value = condition['result']
        line_res[key] = value


def process_usual_location(prefix, conditions, line_res):
    for condition in conditions:
        key = "%s-%s-%s" % (prefix, condition['dim_type'], \
                            condition['unit'])
        value = condition['result']
        line_res[key] = value


def process_keyword(prefix, conditions, line_res):
    for condition in conditions:
        for keyword in condition['data']:
            key = "%s-%s" % (prefix, keyword)
            value = 1
            if key in line_res:
                line_res[key] += 1
            else:
                line_res[key] = value


def process_android_cheat_app(prefix, conditions, line_res):
    for condition in conditions:
        key = prefix
        value = 1
        if key in line_res:
            line_res[key] += 1
        else:
            line_res[key] = value


def process_ios_cheat_app(prefix, conditions, line_res):
    for condition in conditions:
        key = prefix
        value = 1
        if key in line_res:
            line_res[key] += 1
        else:
            line_res[key] = value


def process_android_emulator(prefix, conditions, line_res):
    for condition in conditions:
        key = prefix
        value = 1
        if key in line_res:
            line_res[key] += 1
        else:
            line_res[key] = value


def process_device_status_abnormal(prefix, conditions, line_res):
    for condition in conditions:
        key = prefix
        value = len(condition['abnormal_tags'])
        if key in line_res:
            line_res[key] += value
        else:
            line_res[key] = value


def process_suspected_team(prefix, conditions, line_res):
    for condition in conditions:
        d = [
            "total_cnt",
            "group_id",
            "black_cnt",
            "grey_cnt",
            "black_rat",
            "grey_rat",
            "degree",
            "total_cnt_two",
            "black_cnt_one",
            "black_cnt_two",
            "black_dst",
            "core_dst"
        ]
        new_prefix = "%s-%s" % (prefix, condition['dim_type'])
        for k in d:
            key = "%s-%s" % (new_prefix, k)
            value = condition[k]
            line_res[key] = value


type_dict = {
    'frequency_one_dim': process_frequency_one_dim,
    'frequency_count': process_frequency_count,
    'frequency_distinct': process_frequency_distinct,
    'black_list': process_black_list,
    'fp_exception': process_fp_exception,
    'grey_list': process_grey_list,
    'fuzzy_black_list': process_fuzzy_black_list,
    'geo_ip_distance': process_geo_ip_distance,
    'proxy_ip': process_proxy_ip,
    'gps_distance': process_gps_distance,
    'regex': process_regex,
    'event_time_diff': process_event_time_diff,
    'time_diff': process_time_diff,
    'active_days_two': process_active_days_two,
    'cross_event': process_cross_event,
    'cross_velocity_one_dim': process_cross_velocity_one_dim,
    'cross_velocity_count': process_cross_velocity_count,
    'cross_velocity_distinct': process_cross_velocity_distinct,
    'calculate': process_calculate,
    'last_match': process_last_match,
    'min_max': process_min_max,
    'count': process_count,
    'association_partner': process_association_partner,
    'discredit_count': process_discredit_count,
    'cross_partner': process_cross_partner,
    'four_calculation': process_four_calculation,
    'function_kit': process_function_kit,
    'usual_browser': process_usual_browser,
    'usual_device': process_usual_device,
    'usual_location': process_usual_location,
    'keyword': process_keyword,
    'android_cheat_app': process_android_cheat_app,
    'ios_cheat_app': process_ios_cheat_app,
    'android_emulator': process_android_emulator,
    'device_status_abnormal': process_device_status_abnormal,
    'suspected_team': process_suspected_team
}

def process_rule(rule_id, conditions, rule_type,line_res):
    prefix = "%s-%s"% (rule_id , rule_type)
    if rule_type in type_dict:
        type_dict[rule_type](prefix, conditions, line_res)

def parse(js):
    res = {}
    if js['reason_code'] != "200":
        return res
    rules = js['rules']
    for rule in rules:
        rule_id = rule['rule_id']
        conditions = rule['conditions']
        if len(conditions) == 0:
            continue
        rule_type = conditions[0]['type']
        process_rule(rule_id, conditions, rule_type, res)
    return res