def count_validation(source_df, target_df):
    
    count_validation = []
    count_validation.append('Pass')
    count_validation.append(f'Source_Count= {len(source_df)} , Target_Count = {len(target_df)}')
    if len(source_df) != len(target_df):
        count_validation[0] = 'Fail'

    return count_validation