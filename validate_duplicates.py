def duplicate_validation(source_df,target_df,src_key,tgt_key):

    duplicate_validation=['']

    src_duplicates = source_df[source_df.duplicated()]
    tgt_duplicates = target_df[target_df.duplicated()]

    if len(src_duplicates) == 0 and len(tgt_duplicates) == 0:
        pass
    elif len(src_duplicates) == 0:
        duplicate_validation.append(f'{len(tgt_duplicates)} Extra Duplicate records are populated into target')
    elif len(tgt_duplicates) == 0:
        duplicate_validation.append('Warning : No duplicate records are populated')
    else:
        src_duplicates = src_duplicates.dropna(how='all')
        tgt_duplicates = tgt_duplicates.dropna(how='all')

        grp = src_duplicates.groupby(src_key, dropna=False).value_counts()
        src_groupby = grp.reset_index()

        grp = tgt_duplicates.groupby(tgt_key, dropna=False).value_counts()
        tgt_groupby = grp.reset_index()

        src_groupby[src_key] = src_groupby[src_key].astype(str)
        tgt_groupby[tgt_key] = tgt_groupby[tgt_key].astype(str)

        merge_data = src_groupby.merge(tgt_groupby, left_on=src_key, right_on=tgt_key, how='outer',
                                        indicator=True)

        count_match = merge_data[merge_data['count_x'] == merge_data['count_y']]

        src_grt = merge_data[merge_data['count_x'] > merge_data['count_y']]

        tgt_grt = merge_data[merge_data['count_x'] < merge_data['count_y']]

        if len(count_match) > 0:
            duplicate_validation.append(
                f'WARNING: {len(count_match)} Duplicate records are exactly same in both Source and Target.')
        if len(src_grt) > 0:
            duplicate_validation.append(
                f'WARNING: {len(src_grt)} Duplicate records are partially loaded into Target.')
        if len(tgt_grt) > 0:
            duplicate_validation.append(f'{len(tgt_grt)} Duplicate records are loaded extra into Target.')

        left_in_Src = merge_data[merge_data['_merge'] == 'left_only']
        right_in_tgt = merge_data[merge_data['_merge'] == 'left_only']

        if 'left_only' in merge_data['_merge'].values:
            duplicate_validation.append(f'{len(left_in_Src)} Duplicate records did not get populated in Target.')
        if 'right_only' in merge_data['_merge'].values:
            duplicate_validation.append(f'{len(right_in_tgt)} Extra Duplicate records are populated in Target.')

    if len(duplicate_validation) > 1:
        duplicate_validation.pop(0)

    return duplicate_validation
