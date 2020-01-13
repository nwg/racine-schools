create table schools (
    id serial primary key,
    state_lea_id text,
    state_school_id text,
    nces_lea_id integer,
    nces_school_id integer,
    nces_id bigint unique,
    pss_ppin text unique,
    longname text unique,
    is_elementary boolean not null,
    is_middle boolean not null,
    is_high boolean not null,
    low_grade character(2) check (low_grade in ('PK', 'K3', 'K4', 'K5', 'KG', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13')),
    high_grade character(2) check (low_grade in ('PK', 'K3', 'K4', 'K5', 'KG', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13')),
    UNIQUE (state_lea_id, state_school_id),
    UNIQUE (nces_lea_id, nces_school_id)
);

