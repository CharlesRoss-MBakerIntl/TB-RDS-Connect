
# Create Source Information
source = {
    'table': 'applications_application',
    'name' : 'app',
    'fields': [{'application_number' : 'application_number',
                       'created_at' : 'created_at'}],
    'project': 34,
    'order': 'id'
}


# Create Join List
join_list = [
    
    {'name': 'hotel_name',
     'question_source': 'JOIN_SOURCE',
     'source_id': 'id',
     'join_id': 'application_id',
     'data_source': 'application_data_textboxanswer',
     'question_id': 1015,
     'fields' : [{'value':'hotel_name'}],
     'clean' : [{'hotel_name': ['NULL']}]
    },

    {'name': 'hotel_address',
     'question_source': 'DATA_SOURCE',
     'source_id': 'repeating_answer_section_id',
     'join_id': 'repeating_answer_section_id',
     'question_id': 1016,
     'data_source': 'application_data_addressanswer',
     'fields': [{'line1':'hotel_address_line_1',
                 'line2': 'hotel_address_line_2',
                 'city' : 'hotel_city',
                 'state': 'hotel_state',
                 'zip' : 'hotel_zip'}],
     'clean' : [{'hotel_address_line_1': ['NULL']},
                {'hotel_city': ['NULL']},
                {'hotel_state': ['NULL']},
                {'hotel_zip': ['NULL']}]
    },

    {'name': 'hotel_status',
     'question_source': 'DATA_SOURCE',
     'source_id': 'repeating_answer_section_id',
     'join_id': 'repeating_answer_section_id',
     'data_source': 'application_data_singleselectanswer',
     'question_id': 1013,
     'fields': [{'value':'hotel_status'}],
     'clean' : []
    },

    {'name': 'license_in',
     'question_source': 'DATA_SOURCE',
     'source_id': 'repeating_answer_section_id',
     'join_id': 'repeating_answer_section_id',
     'data_source': 'application_data_dateanswer',
     'question_id': 1021,
     'fields': [{'value':'license_in'}],
     'clean' : [{'license_in': ['DATE_CONVERT']}]
    },

    {'name': 'license_out',
     'question_source': 'DATA_SOURCE',
     'source_id': 'repeating_answer_section_id',
     'join_id': 'repeating_answer_section_id',
     'data_source': 'application_data_dateanswer',
     'question_id': 1022,
     'fields': [{'value':'license_out'}],
     'clean' : [{'license_out': ['DATE_CONVERT']}]
    },

    {'name': 'total_in_household',
     'question_source': 'JOIN_SOURCE',
     'source_id': 'id',
     'join_id': 'application_id',
     'data_source': 'application_data_numberanswer',
     'question_id': 596,
     'fields': [{'value':'total_in_household'}],
     'clean' : [{'total_in_household': ['INT_CONVERT']}]
    }
]


# Create Query Package
query_package = {'source':source, 'join_list': join_list}


# Function to Return Package
def get_query_package():
    return query_package