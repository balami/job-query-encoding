import psycopg2 as pg
from psycopg2 import sql
from dbconnection import dbconnect
import category_encoders as ce
import pandas as pd
import json
import glob, os, re, math

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
            query_dict[filename] = trimmed_contents
    return query_dict

def parse_queries():
    for filename, query_text in query_dict.items():
        print('Reading query...',filename)
        or_predicate_set = []
        final_dict = {}
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
        # print('\nQuery Number:',filename,'\n','Table Set:',table_set,'\n Join Set:',join_set,'\n AND Predicate Set:',and_predicate_set,'\n OR Predicate Set:',or_predicate_set,'\n Nested OR Predicate Set:',and_or_predicate_set,'\nCardinality:',cardinality,'\n')
        final_dict.update({'Query Number': filename,'Tables': table_set, 'Joins': join_set, 'AND Predicate': and_predicate_set,'OR Predicate': or_predicate_set,'Nested OR Predicate': and_or_predicate_set,'Cardinality': cardinality})
        write_json_file(final_dict,filename)

def write_json_file(final_dict,filename):
    filename_json = filename.replace('.sql','.json')
    with open(filename_json, 'w', encoding='utf-8') as f:
        json.dump(final_dict, f, ensure_ascii=False, indent=4)

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
        for opmatch in re.finditer('AND\s\((.+?OR.+?)\)\sAND',modified_query):
            match_group = opmatch.group()
            match_block = match_group.replace('AND (','').replace(') AND','')
            or_query_blocks = match_block.split('OR')
            left_block = or_query_blocks[0].strip()
            right_block = or_query_blocks[1].strip()
            if(left_block.startswith('(') and left_block.endswith(')')):
                left_block = left_block[1:-1]
            if(right_block.startswith('(') and right_block.endswith(')')):
                right_block = right_block[1:-1]
            if((('AND').center(5) in left_block) or (('AND').center(5) in right_block)):
                if(('AND').center(5) in left_block):
                    and_group = left_block.split('AND')
                    or_col = right_block
                elif(('AND').center(5) in right_block):
                    and_group = right_block.split('AND')
                    or_col = left_block
                and_group1 = and_group[0].strip()
                and_group2 = and_group[1].strip()
                nested_blocks = [and_group1,and_group2,or_col]
                for item in nested_blocks:
                    opt = check_operator(item)
                    and_or_predicate_set = get_or_predicate_set(item,opt,tbl_ref_list,and_or_predicate_set)
            else:
                #not a nested OR case
                for or_block in or_query_blocks:
                    or_block = or_block.strip()
                    opt_type = check_operator(or_block)
                    or_predicate_set = get_or_predicate_set(or_block,opt_type,tbl_ref_list,or_predicate_set)
            modified_query = modified_query.replace(match_group,'AND').strip()
    return modified_query, or_predicate_set, and_or_predicate_set

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
    if('=' in query_block) and ('>' not in query_block) and ('<' not in query_block) and ('!' not in query_block):
        operator_type = '='
    elif('>' in query_block):
        if('=' in query_block):
            operator_type = '>='
        operator_type = '>'
    elif('<' in query_block):
        if('=' in query_block):
            operator_type = '<='
        operator_type = '<'
    elif('!' in query_block) and ('=' in query_block):
        operator_type = '!='
    elif('LIKE' in query_block):
        if('NOT' in query_block):
            operator_type = 'NOT LIKE'
        else:
            operator_type = 'LIKE'
    elif('IS' in query_block):
        if('NOT' in query_block):
            operator_type = 'IS NOT'
        else:
            operator_type = 'IS'
    elif('IN',query_block):
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
    and_block = and_block.strip()
    if(and_block.startswith('(') and and_block.endswith(')')):
        and_block = and_block.replace('(','').replace(')','')
    sub_blocks = and_block.split(operator_type)
    col_alias = sub_blocks[0].strip()
    col_left = get_column_name(col_alias,tbl_ref_list)
    value = sub_blocks[1].strip()
    if(value.isdigit()):
        value = get_normalized_value(col_left,value)
    if((operator_type =='=') and (type(value) is str)):
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
    log_actual_rows = math.log10(actual_rows)
    selectivity_val = normalize_selectivity_val(log_actual_rows)
    return selectivity_val

def normalize_selectivity_val(log_actual_rows):
    sel_val = (log_actual_rows-min_cardinality)/(max_cardinality-min_cardinality)
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
        # makecsv(encoded_col_df,"encoded_col_vectors.csv")
        encoded_col_dict = encoded_col_df.set_index('column_name').T.to_dict('list')
        tbl_df = table_df()
        encoded_tbl_df = encode_df(tbl_df)
        # makecsv(encoded_tbl_df,"encoded_tbl_vectors.csv")
        encoded_tbl_dict = encoded_tbl_df.set_index('table_name').T.to_dict('list')
        op_df = operator_df()
        encoded_operator_df = encode_df(op_df)
        # makecsv(encoded_operator_df,"encoded_op_vectors.csv")
        encoded_op_dict = encoded_operator_df.set_index('operator_type').T.to_dict('list')
        query_dict = get_queries()
        max_cardinality, min_cardinality = get_logcardinalities()
        parse_queries()
        dbdisconnect()
