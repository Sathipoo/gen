IIF(
    TO_DATE(IN_UTC_DATETIME, 'YYYY-MM-DD HH24:MI:SS') >= 
        ADD_TO_DATE(
            ADD_TO_DATE(
                TO_DATE(TO_CHAR(GET_DATE_PART(TO_DATE(IN_UTC_DATETIME, 'YYYY-MM-DD HH24:MI:SS'), 'YYYY')) || '-03-01', 'YYYY-MM-DD'),
                'DD',
                IIF(
                    MOD(TRUNC((TO_DATE(TO_CHAR(GET_DATE_PART(TO_DATE(IN_UTC_DATETIME, 'YYYY-MM-DD HH24:MI:SS'), 'YYYY')) || '-03-01', 'YYYY-MM-DD') - TO_DATE('1970-01-01', 'YYYY-MM-DD'))), 7) <= 3,
                    3 - MOD(TRUNC((TO_DATE(TO_CHAR(GET_DATE_PART(TO_DATE(IN_UTC_DATETIME, 'YYYY-MM-DD HH24:MI:SS'), 'YYYY')) || '-03-01', 'YYYY-MM-DD') - TO_DATE('1970-01-01', 'YYYY-MM-DD'))), 7),
                    10 - MOD(TRUNC((TO_DATE(TO_CHAR(GET_DATE_PART(TO_DATE(IN_UTC_DATETIME, 'YYYY-MM-DD HH24:MI:SS'), 'YYYY')) || '-03-01', 'YYYY-MM-DD') - TO_DATE('1970-01-01', 'YYYY-MM-DD'))), 7)
                )
            ),
            'DD', 7  -- Second Sunday
        ) + ADD_TO_DATE(TO_DATE('1970-01-01', 'YYYY-MM-DD'), 'HH', 7)  -- 2 AM EST to 7 AM UTC
    AND 
    TO_DATE(IN_UTC_DATETIME, 'YYYY-MM-DD HH24:MI:SS') < 
        ADD_TO_DATE(
            TO_DATE(TO_CHAR(GET_DATE_PART(TO_DATE(IN_UTC_DATETIME, 'YYYY-MM-DD HH24:MI:SS'), 'YYYY')) || '-11-01', 'YYYY-MM-DD'),
            'DD',
            IIF(
                MOD(TRUNC((TO_DATE(TO_CHAR(GET_DATE_PART(TO_DATE(IN_UTC_DATETIME, 'YYYY-MM-DD HH24:MI:SS'), 'YYYY')) || '-11-01', 'YYYY-MM-DD') - TO_DATE('1970-01-01', 'YYYY-MM-DD'))), 7) <= 3,
                3 - MOD(TRUNC((TO_DATE(TO_CHAR(GET_DATE_PART(TO_DATE(IN_UTC_DATETIME, 'YYYY-MM-DD HH24:MI:SS'), 'YYYY')) || '-11-01', 'YYYY-MM-DD') - TO_DATE('1970-01-01', 'YYYY-MM-DD'))), 7),
                10 - MOD(TRUNC((TO_DATE(TO_CHAR(GET_DATE_PART(TO_DATE(IN_UTC_DATETIME, 'YYYY-MM-DD HH24:MI:SS'), 'YYYY')) || '-11-01', 'YYYY-MM-DD') - TO_DATE('1970-01-01', 'YYYY-MM-DD'))), 7)
            )
        ) + ADD_TO_DATE(TO_DATE('1970-01-01', 'YYYY-MM-DD'), 'HH', 6)  -- 2 AM EDT to 6 AM UTC
    ,
    ADD_TO_DATE(TO_DATE(IN_UTC_DATETIME, 'YYYY-MM-DD HH24:MI:SS'), 'HH', -4),  -- EDT (UTC-4)
    ADD_TO_DATE(TO_DATE(IN_UTC_DATETIME, 'YYYY-MM-DD HH24:MI:SS'), 'HH', -5)   -- EST (UTC-5)
)
