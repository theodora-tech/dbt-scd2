{{ config(materialized='incremental', unique_key =  'UserId', incremental_strategy='delete+insert') }}
{{
    create_scd2_table(
        {
            'AdvisorId': {
                'UserCreatedEvent': '"advisorId"'
            },
            'ApplicationTypeId': {
                'UserCreatedEvent': '"clientApplicationId"'
            },
            'ExternalReferenceId': {
                'UserCreatedEvent': '"externalReferenceId"'
            },
            'IsWaitListed': {
                'UserCreatedEvent': '"isWaitListed"'
            },
            'Locale': {
                'LocaleChangedEvent': '"locale"',
                'UserCreatedEvent': '"locale"'
            },
            'Msisdn': {
                'UserCreatedEvent': '"msisdn"'
            },
            'PhoneNumberCountryId': {
                'UserCreatedEvent': '"countryId"'
            },
            'UserId': {
                'LocaleChangedEvent': '"userId"',
                'UserCreatedEvent': '"userId"'
            },
            'UserCreatedTime': {
                'UserCreatedEvent': '"EventCreated"'
            }
        },
        'UserId',
        'UserKey'
    )
}}