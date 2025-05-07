

def transpose_it(df_ta, ta_results_column_list, y_column_name = 'mid_c'):

    y = df_ta[y_column_name].values
    
    t = df_ta.transpose()
    column_list = t.columns
    t['combined']= t.values.tolist()
    t.drop(columns = column_list, inplace = True)

    y_list = []
    for i in range(0, len(t.index)):
        y_list.append(y)

    t[y_column_name] = y_list

    t.reset_index(inplace = True)
    t = t[t['index'].isin(ta_results_column_list)]

    return t
