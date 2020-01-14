create table education_levels (
    level integer primary key,
    level_name text
);

insert into education_levels values
    (3, $$Associate degree$$),
    (4, $$Bachelor's degree$$),
    (5, $$Master's degree$$),
    (6, $$6-year Specialist's degree$$),
    (7, $$Doctorate$$),
    (8, $$Other$$);

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

create table if not exists appointments (
    id serial primary key,
    staff_id text,
    year integer not null,
    first_name text,
    last_name text,
    state_lea_id text not null,
    state_school_id text not null,
    position_code integer,
    position_category text,
    gender text check (gender in ('M', 'F')),
    education_level integer references education_levels (level),
    fte real
);

create table appointments_imported (
    year integer primary key
);

create table discipline_counts (
    nces_id bigint not null,
    year integer not null,
    category text not null,
    sex character(1) not null,
    american_indian_or_alaska_native text not null,
    asian text not null,
    hawaiian_or_pacific_islander text not null,
    hispanic text not null,
    black text not null,
    white text not null,
    two_or_more_races text not null,
    total_idea_only text,
    total_504_only text,
    total text not null,
    less_lep text not null,
    UNIQUE (nces_id, year, category, sex)
);

create table discipline_counts_imported (
    year integer primary key
);
