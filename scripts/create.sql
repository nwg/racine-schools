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

create table position_category_rank (
    rank integer,
    position_category text primary key
);

insert into position_category_rank values
    (1, $$Other$$),
    (2, $$Pupil Services$$),
    (3, $$Aides / Paraprofessionals$$),
    (4, $$Teachers$$),
    (5, $$Administrators$$);

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
    is_private boolean not null,
    low_grade character(2) check (low_grade in ('PK', 'K3', 'K4', 'K5', 'KG', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13')),
    high_grade character(2) check (low_grade in ('PK', 'K3', 'K4', 'K5', 'KG', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13')),
    address1 text,
    address2 text,
    address_comment text,
    phone text,
    second_address1 text,
    second_address2 text,
    second_address_grades text,
    second_phone text,
    second_address_comment text,
    website text,
    mission text,
    report1 text,
    report2 text,
    affiliation text,
    logo text,
    disadvantaged_pct NUMERIC(5, 2),
    curriculum_focus text,
    choice_students_pct NUMERIC(5, 2),
    num_students integer,
    num_grade_levels integer,
    is_old_siena boolean not null,
    summary_year integer not null,
    UNIQUE (state_lea_id, state_school_id),
    UNIQUE (nces_lea_id, nces_school_id)
);

create table nces_id_history (
    school_id integer references schools (id),
    nces_id bigint unique,
    end_year integer,
    UNIQUE (school_id, nces_id)
);

create table state_school_id_history (
    school_id integer references schools (id),
    state_school_id text unique,
    end_year integer,
    UNIQUE (school_id, state_school_id)
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
    position_category text not null,
    gender text check (gender in ('M', 'F')),
    education_level integer references education_levels (level),
    fte real
);

create table appointments_distinct_ranked_most_recent (
    id serial primary key,
    year integer not null,
    state_lea_id text not null,
    state_school_id text not null,
    first_name text,
    last_name text,
    position_category text,
    gender text check (gender in ('M', 'F')),
    education_level integer references education_levels (level)
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

create table if not exists nces_enrollment_counts (
    nces_id bigint not null,
    year integer not null,
    grade character(2) check (grade in ('PK', 'K3', 'K4', 'KG', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', 'UE', 'US', 'UG', 'AE')),
    sex character(1) not null check (sex in ('M', 'F')),
    american_indian_or_alaska_native text not null,
    asian text not null,
    hawaiian_or_pacific_islander text not null,
    hispanic text not null,
    black text not null,
    white text not null,
    two_or_more_races text not null,
    total text not null,
    UNIQUE (nces_id, year, grade, sex)
);

create table pss_religious_orientation (
    category integer primary key,
    label text
);

insert into pss_religious_orientation values
    (1, 'Roman Catholic'),
    (2, 'African Methodist Episcopal'),
    (3, 'Amish'),
    (4, 'Assembly of God'),
    (5, 'Baptist'),
    (6, 'Brethren'),
    (7, 'Calvinist'),
    (8, 'Christian (no specific denomination)'),
    (9, 'Church of Christ'),
    (10, 'Church of God'),
    (11, 'Church of God in Christ'),
    (12, 'Church of the Nazarene'),
    (13, 'Disciples of Christ'),
    (14, 'Episcopal'),
    (15, 'Friends'),
    (16, 'Greek Orthodox'),
    (17, 'Islamic'),
    (18, 'Jewish'),
    (19, 'Latter Day Saints'),
    (20, 'Lutheran Church - Missouri Synod'),
    (21, 'Evangelical Lutheran Church in America'),
    (22, 'Wisconsin Evangelical Luteran Synod'),
    (23, 'Other Lutheran'),
    (24, 'Mennonite'),
    (25, 'Methodist'),
    (26, 'Pentecostal'),
    (27, 'Presbyterian'),
    (28, 'Seventh-Day Adventist'),
    (29, 'Other');


create table pss_info (
    ppin text primary key,
    year integer not null,
    kg_hours integer,
    kg_days_per_week integer,
    is_religious bool not null,
    religious_orientation integer references pss_religious_orientation (category),
    days_in_year integer,
    hours_in_day integer,
    minutes_in_day integer,
    num_students integer,
    num_fte_teachers numeric(4, 1),
    enrollment integer,
    UNIQUE (ppin, year)
);

create table pss_enrollment_grade_counts (
    ppin text not null,
    year integer not null,
    grade character(2) not null check (grade in ('PK', 'K3', 'K4', 'KG', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', 'UE', 'US', 'UG', 'AE')),
    enrollment integer not null,
    UNIQUE (ppin, year, grade)
);

create table pss_enrollment_demographic_counts (
    ppin text,
    year integer not null,
    american_indian_or_alaska_native integer not null,
    asian integer not null,
    hawaiian_or_pacific_islander integer not null,
    hispanic integer not null,
    black integer not null,
    white integer not null,
    two_or_more_races integer not null,
    male integer not null,
    female integer not null,
    total integer not null,
    UNIQUE (ppin, year)
);
