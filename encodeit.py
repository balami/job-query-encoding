import psycopg2 as pg
from psycopg2 import sql
import category_encoders as ce
import pandas as pd
import json
import glob, os, re, math

def dbconnect():
    conn = pg.connect(user="", password="", host="", port="", database="")
    cur = conn.cursor()
    print("Connected to database\n")
    return cur, conn

def dbdisconnect():
    cur.close()
    conn.close()
    print("Disconnected from database\n")

def column_df():
    cur.execute("SELECT t.table_name, array_agg(c.column_name::text) as columns \
                    FROM information_schema.tables t inner join information_schema.columns c on t.table_name = c.table_name \
                    WHERE t.table_schema = 'public' and t.table_type= 'BASE TABLE' and c.table_schema = 'public' \
                    GROUP BY t.table_name;")
    results = cur.fetchall()
    all_columns_list =[]
    for row in results:
        col_name = [row[0]+'.'+col for col in row[1]]
        all_columns_list.append(sorted(col_name))
    all_columns = [val for sublist in all_columns_list for val in sublist]
    col_df = pd.DataFrame(all_columns, columns=['column_name'])
    return col_df

def table_df():
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' GROUP BY table_name")
    results =  cur.fetchall()
    tbl_df = pd.DataFrame(results, columns=['table_name'])
    return tbl_df

def operator_df():
    operator_list = ('=','>','<','!=','>=','<=','LIKE','NOT LIKE','IS','IS NOT')
    op_df = pd.DataFrame(operator_list, columns=['operator_type'])
    return op_df

def encode_df(df):
    bi_encoder = ce.BinaryEncoder(df)
    df_binary = bi_encoder.fit_transform(df)
    df['bcode'] = df_binary.applymap(str).apply(lambda x: ''.join(x), axis=1)
    return df

def makecsv(df,filename):
    df.to_csv(filename, index = False, header = False)

def new_queryfile(filename,query_text,where_block,and_predicate_block):
    new_sql_query = query_text.replace(where_block,and_predicate_block)
    with open(filename, 'w') as mysqlfile:
        print('Writing to file')
        mysqlfile.write(new_sql_query)

def get_queries():
    query_dict = {}
    #read all query files in the JOB
    allfiles = glob.glob('queries' + '/*.sql', recursive=True)
    for queryfile in allfiles:
        filename = os.path.basename(queryfile)
        with open(queryfile, 'r') as f:
            file_contents = f.read()
            #remove the line breaks, tabs and extra spaces
            trimmed_contents = ' '.join(file_contents.split())
            #remove all MIN from the select statement if any
            min_match = re.search("\sMIN(.+?)(\))",trimmed_contents)
            if(min_match):
                trimmed_contents = trimmed_contents.replace('MIN','')
            #make sure each opeartor (=,>,<,>=,<=,,!=) are separated by a leading and trailing whitespace character
            op_list = ['\s=\S','\s>\S','\s<\S','\s!=\S','\s>=\S','\s<=\S']
            new_trimmed_contents = add_lead_trail_spaces(op_list, trimmed_contents)
            #store the query filename and the cleaned queries in a dictionary
            query_dict[filename] = new_trimmed_contents
    return query_dict

def add_lead_trail_spaces(op_list, trimmed_contents):
    for opr in op_list:
        for opmatch in re.finditer(opr, trimmed_contents):
            match_group = opmatch.group()
            first_character = match_group[0]
            last_character = match_group[-1]
            text_to_replace = match_group.replace(last_character,' '+ last_character)
            trimmed_contents = trimmed_contents.replace(match_group,text_to_replace)
    return trimmed_contents

def parse_queries():
    for filename, query_text in query_dict.items():
        print('Reading query...',filename)
        or_predicate_set = []
        cardinality = get_estimate(query_text)
        from_query_block = re.search("FROM(.+?)WHERE", query_text)
        from_block = from_query_block.group(1).strip()
        table_set, tbl_ref_list = get_table_set(from_block)
        where_query_block = re.search("WHERE(.+?);", query_text)
        where_block = where_query_block.group(1)
        modified_query = rem_betweens(where_block)
        modified_subquery, or_predicate_set, and_or_predicate_set = or_predicate_block(modified_query,tbl_ref_list,or_predicate_set)
        #To write the modified query to file
        # new_queryfile(filename,query_text,where_block,and_predicate_block)
        join_set, and_predicate_set, or_predicate_set = get_all_sets(modified_subquery,tbl_ref_list,or_predicate_set)
        print('\nQuery Number:',filename,'\n','Table Set:',table_set,'\n Join Set:',join_set,'\n AND Predicate Set:',and_predicate_set,'\n OR Predicate Set:',or_predicate_set,'\n Nested OR Predicate Set:',and_or_predicate_set,'\nCardinality:',cardinality,'\n')
        write_json_file(table_set)

def write_json_file(set):
    with open('sets.json', 'a', encoding='utf-8') as f:
        json.dump(set, f, ensure_ascii=False, indent=4)

def get_table_set(from_block):
    table_set = []
    tbl_ref_list ={}
    from_block = from_block.split(',')
    for element in from_block:
        item = element.split('AS')
        item = [x.strip(' ') for x in item]
        tbl_name = item[0]
        tbl_alias = item[1]
        tbl_ref_list[tbl_alias] = tbl_name
        if tbl_name in encoded_tbl_dict:
            table_set.append(encoded_tbl_dict[tbl_name])
    # print(tbl_ref_list)
    return table_set, tbl_ref_list

def rem_betweens(where_block):
    if('BETWEEN' in where_block):
        for block in re.finditer("(\S+)\s+BETWEEN(.+?)AND(.+?)\s", where_block):
            opmatch = block.group()
            match_group = opmatch.split('BETWEEN')
            match_group = [x.strip() for x in match_group]
            col_alias = match_group[0]
            values = match_group[1].split('AND')
            pred_left = col_alias+('>=').center(4)+values[0]
            pred_right = col_alias+('<=').center(3)+values[1]
            predicate_block = pred_left +('AND').center(4)+pred_right
            where_block = where_block.replace(opmatch,predicate_block.ljust(len(predicate_block)+1))
    return where_block

def or_predicate_block(modified_query,tbl_ref_list,or_predicate_set):
    and_or_predicate_set = []
    or_match = re.search('\sAND(\s\()',modified_query)
    if(or_match):
        for opmatch in re.finditer('\s(AND\s\(.+\)\sAND)\s',modified_query):
            match_group = opmatch.group()
            if('OR'.center(4) in match_group):
                begining_subpart_rem = ('AND'.center(5))+'('
                ending_subpart_rem = ')'+('AND'.center(5))
                match_block = match_group.replace(begining_subpart_rem,'').replace(ending_subpart_rem,'')
                or_query_blocks = match_block.split('OR')
                lhs = or_query_blocks[0]
                rhs = or_query_blocks[1]
                if((('AND').center(5) in lhs) or (('AND').center(5) in rhs)):
                    #go to or and predicate set function
                    if(('AND').center(5) in lhs):
                        lhs = lhs.replace('(','').replace(')','')
                        and_group = lhs.split('AND')
                        or_col = rhs
                    else:
                        rhs = rhs.replace('(','').replace(')','')
                        and_group = rhs.split('AND')
                        or_col = lhs
                    first_grp = and_group[0].strip()
                    second_grp = and_group[1].strip()
                    or_col = or_col.strip()
                    nested_blocks = [first_grp,second_grp,or_col]
                    for item in nested_blocks:
                        opt = check_operator(item)
                        and_or_predicate_set = get_and_or_predicate_set(item,opt,tbl_ref_list,and_or_predicate_set)
                else:
                    for or_block in or_query_blocks:
                        or_block = or_block.strip()
                        opt_type = check_operator(or_block)
                        or_predicate_set = get_or_predicate_set(or_block,opt_type,tbl_ref_list,or_predicate_set)
                modified_query = modified_query.replace(match_group,'AND'.center(5))
            else:
                match_block = match_group.replace(' (',' ').replace(') ',' ')
                modified_query = modified_query.replace(match_group,match_block)
    return modified_query, or_predicate_set, and_or_predicate_set

def get_and_or_predicate_set(item,operator_type,tbl_ref_list,and_or_predicate_set):
    vals = item.split(operator_type)
    col_id = vals[0].strip()
    col_left = get_column_name(col_id,tbl_ref_list)
    value = vals[1].strip()
    if(value.isdigit()):
        value = get_normalized_value(col_left,value)
    and_or_predicate_set.append(str(encoded_col_dict[col_left][0])+','+str(encoded_op_dict[operator_type][0])+','+str(value))
    return and_or_predicate_set

def get_or_predicate_set(new_or_block,operator_type,tbl_ref_list,or_predicate_set):
    vals = new_or_block.split(operator_type)
    col_id = vals[0].strip()
    col_left = get_column_name(col_id,tbl_ref_list)
    value = vals[1].strip()
    if(value.isdigit()):
        value = get_normalized_value(col_left,value)
    or_predicate_set.append(str(encoded_col_dict[col_left][0])+','+str(encoded_op_dict[operator_type][0])+','+str(value))
    return or_predicate_set

def get_all_sets(modified_subquery,tbl_ref_list,or_predicate_set):
    print('Processing query...')
    join_set =[]
    and_predicate_set = []
    and_block_list = modified_subquery.split('AND')
    and_block_list = [x.strip() for x in and_block_list]
    for and_block in and_block_list:
        operator_type = check_operator(and_block)
        if(operator_type=='IN'):
             or_predicate_set = breakdown_inblock(and_block,tbl_ref_list,or_predicate_set)
        else:
            get_query_sets(operator_type,and_block,join_set,and_predicate_set,tbl_ref_list)
    return join_set, and_predicate_set, or_predicate_set

def check_operator(query_block):
    if(re.search('\s(=)\s',query_block)):
        operator_type = '='
    elif(re.search('\s(>)\s',query_block)):
        operator_type = '>'
    elif(re.search('\s(<)\s',query_block)):
        operator_type = '<'
    elif(re.search('\s(>=)\s',query_block)):
        operator_type = '>='
    elif(re.search('\s(<=)\s',query_block)):
        operator_type = '<='
    elif(re.search('\s(!=)\s',query_block)):
        operator_type = '!='
    elif(re.search('\s(LIKE)\s',query_block)):
        if(re.search('\s(NOT\sLIKE)\s',query_block)):
            operator_type = 'NOT LIKE'
        else:
            operator_type = 'LIKE'
    elif(re.search('\s(IS)\s',query_block)):
        if(re.search('\s(IS\sNOT)\s',query_block)):
            operator_type = 'IS NOT'
        else:
            operator_type = 'IS'
    elif(re.search('\s(IN)\s',query_block)):
        operator_type = 'IN'
    else:
        print('Operator not found in ',query_block)
        quit()
    return operator_type

def breakdown_inblock(and_block,tbl_ref_list,or_predicate_set):
    sub_blocks = and_block.split('IN')
    col_id = sub_blocks[0].strip()
    listofitems = sub_blocks[1].strip().replace('(','').replace(')','')
    listvalues = listofitems.split(',')
    new_or_block = []
    for item in listvalues:
        new_or_block = col_id+'='.center(3)+item
        operator_type = '='
        or_predicate_set = get_or_predicate_set(new_or_block,operator_type,tbl_ref_list,or_predicate_set)
    return or_predicate_set

def get_query_sets(operator_type,and_block,join_set,and_predicate_set,tbl_ref_list):
    sub_blocks = and_block.split(operator_type)
    col_alias = sub_blocks[0].strip()
    col_left = get_column_name(col_alias,tbl_ref_list)
    value = sub_blocks[1].strip()
    if(value.isdigit()):
        value = get_normalized_value(col_left,value)
    if((operator_type =='=') and (not(value.isdigit()))):
        reg_match_char = re.search('(\w*\.)',value)
        if(reg_match_char):
             col_right = get_column_name(value,tbl_ref_list)
             if(col_right in encoded_col_dict.keys()):
                 get_join_set(col_left,operator_type,col_right,join_set)
             else:
                 print('couldnt find join pair for', col_left,'and',col_right)
        else:
            get_and_predicate_set(col_left,operator_type,value,and_predicate_set)
    else:
        get_and_predicate_set(col_left,operator_type,value,and_predicate_set)

def get_normalized_value(col_left,value):
    tbl, separator, col = col_left.partition('.')
    cur.execute(sql.SQL("SELECT MAX({}), MIN({}) FROM {}").format(sql.Identifier(col.strip()),sql.Identifier(col.strip()),sql.Identifier(tbl.strip())))
    results =  cur.fetchall()
    max_value = int(results[0][0])
    min_value = int(results[0][1])
    val_normalized = round((int(value)-min_value)/(max_value-min_value),10)
    return val_normalized;

def get_column_name(alias,tbl_ref_list):
    match_char = re.search('(\w*\.)',alias)
    match_char_group = match_char.group(1)
    col_alias = match_char_group[:-1]
    if(col_alias in tbl_ref_list.keys()):
        to_replace = tbl_ref_list[col_alias]+'.'
        col_name = alias.replace(match_char_group,to_replace).strip()
    else:
        print('Coudnt find key in the alias dictionary for',alias)
    return col_name

def get_join_set(col_left,operator_type,col_right,join_set):
    #make sure columns are alphabetically sorted before encoding
    if(col_left<col_right):
        join_set.append(str(encoded_col_dict[col_left][0])+str(encoded_col_dict[col_right][0]))
    else:
        join_set.append(str(encoded_col_dict[col_right][0])+str(encoded_col_dict[col_left][0]))

def get_and_predicate_set(col_alias,operator_type,value,and_predicate_set):
    and_predicate_set.append(str(encoded_col_dict[col_alias][0])+','+str(encoded_op_dict[operator_type][0])+','+str(value))

def get_estimate(query_text):
    query_to_run = 'EXPLAIN ANALYZE '+ query_text
    cur.execute(query_to_run)
    records = cur.fetchall()
    res = str(records).split(" ")
    indices = [res.index(i) for i in res if 'rows' in i]
    actual_ind = indices[1]
    actual_rows = int(re.search(r'\d+', res[actual_ind]).group())
    selectivity_val = normalize_selectivity_val(actual_rows)
    return selectivity_val

def normalize_selectivity_val(actual_rows):
    sel_val = (actual_rows-min_cardinality)/(max_cardinality-1)
    return sel_val;

def get_logcardinalities():
    #get cardinalities of all queries and take logarithm of them
    log_cardinalities = []
    for query in query_dict.keys():
        cur.execute('EXPLAIN ANALYZE '+ query_dict[query])
        records = cur.fetchall()
        res = str(records).split(" ")
        indices = [res.index(i) for i in res if 'rows' in i]
        actual_ind = indices[1]
        totalrows = int(re.search(r'\d+', res[actual_ind]).group())
        if(totalrows != 0):
            log_val = math.log10(totalrows)
        else:
            log_val = 0
        log_cardinalities.append(log_val)
    max_cardinality = max(log_cardinalities)
    min_cardinality = min(log_cardinalities)
    return max_cardinality, min_cardinality

if __name__ == "__main__":
    global cur, encoded_col_dict, encoded_tbl_dict, encoded_op_dict, query_dict, max_cardinality, min_cardinality
    cur, conn = dbconnect()
    if(conn):
        col_df = column_df()
        encoded_col_df = encode_df(col_df)
        makecsv(encoded_col_df,"encoded_col_vectors.csv")
        encoded_col_dict = encoded_col_df.set_index('column_name').T.to_dict('list')
        tbl_df = table_df()
        encoded_tbl_df = encode_df(tbl_df)
        # makecsv(encoded_tbl_df,"encoded_tbl_vectors.csv")
        encoded_tbl_dict = encoded_tbl_df.set_index('table_name').T.to_dict('list')
        op_df = operator_df()
        encoded_op_df = encode_df(op_df)
        # makecsv(encoded_operator_df,"encoded_op_vectors.csv")
        encoded_op_dict = encoded_op_df.set_index('operator_type').T.to_dict('list')
        query_dict = get_queries()
        max_cardinality, min_cardinality = get_logcardinalities()
        parse_queries()
        dbdisconnect()
