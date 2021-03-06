.DEFAULT_GOAL := download

GEO_2017_URL := https://nces.ed.gov/programs/edge/data/EDGE_GEOCODE_PUBLICSCH_1718.zip
GEO_2017_ZIP := $(notdir $(GEO_2017_URL))
GEO_2017_FILE := EDGE_GEOCODE_PUBLICSCH_1718/EDGE_GEOCODE_PUBLICSCH_1718.TXT
GEO_2017_FINAL := nces_schools_geo_2017.csv

LUNCH_2017_URL := https://nces.ed.gov/ccd/Data/zip/ccd_sch_033_1718_l_1a_083118.zip
LUNCH_2017_ZIP := $(notdir $(LUNCH_2017_URL))
LUNCH_2017_FILE := ccd_sch_033_1718_l_1a_083118.csv
LUNCH_2017_FINAL := nces_schools_lunch_2017.cav

CHARACTERISTICS_2017_URL := https://nces.ed.gov/ccd/Data/zip/ccd_sch_129_1718_w_1a_083118.zip
CHARACTERISTICS_2017_ZIP := $(notdir $(CHARACTERISTICS_2017_URL))
CHARACTERISTICS_2017_FILE := ccd_sch_129_1718_w_1a_083118.csv
CHARACTERISTICS_2017_FINAL := nces_schools_characteristics_2017.cav

MEMBERSHIP_2017_URL := https://nces.ed.gov/ccd/Data/zip/ccd_sch_052_1718_l_1a_083118.zip
MEMBERSHIP_2017_ZIP := $(notdir $(MEMBERSHIP_2017_URL))
MEMBERSHIP_2017_ZIP2 := ccd_SCH_052_1718_l_1a_083118\ CSV.zip
MEMBERSHIP_2017_ZIP2_RENAMED := nces_schools_membership_2017.csv.zip
MEMBERSHIP_2017_FILE := ccd_SCH_052_1718_l_1a_083118.csv
MEMBERSHIP_2017_FINAL := nces_schools_membership_2017.csv

DIRECTORY_2017_URL := https://nces.ed.gov/ccd/Data/zip/ccd_sch_029_1718_w_1a_083118.zip
DIRECTORY_2017_ZIP := $(notdir $(DIRECTORY_2017_URL))
DIRECTORY_2017_FILE := ccd_sch_029_1718_w_1a_083118.csv
DIRECTORY_2017_FINAL := nces_schools_directory_2017.csv

#$(info $(GEO_2017_ZIP))

$(GEO_2017_ZIP):
	curl -O "$(GEO_2017_URL)"

$(GEO_2017_FINAL): $(GEO_2017_ZIP)
	unzip -o $< $(GEO_2017_FILE)
	mv $(GEO_2017_FILE) $(GEO_2017_FINAL)
	touch $(GEO_2017_FINAL)

$(LUNCH_2017_ZIP):
	curl -O "$(LUNCH_2017_URL)"

$(LUNCH_2017_FINAL): $(LUNCH_2017_ZIP)
	unzip -o $< $(LUNCH_2017_FILE)
	mv $(LUNCH_2017_FILE) $@
	touch $@

$(CHARACTERISTICS_2017_ZIP):
	curl -O "$(CHARACTERISTICS_2017_URL)"

$(CHARACTERISTICS_2017_FINAL): $(CHARACTERISTICS_2017_ZIP)
	unzip -o $< $(CHARACTERISTICS_2017_FILE)
	mv $(CHARACTERISTICS_2017_FILE) $@
	touch $@

$(MEMBERSHIP_2017_ZIP):
	curl -O "$(MEMBERSHIP_2017_URL)"

$(MEMBERSHIP_2017_ZIP2_RENAMED): $(MEMBERSHIP_2017_ZIP)
	unzip -o $< $(MEMBERSHIP_2017_ZIP2)
	mv $(MEMBERSHIP_2017_ZIP2) $(MEMBERSHIP_2017_ZIP2_RENAMED)
	touch $(MEMBERSHIP_2017_ZIP2_RENAMED)

$(MEMBERSHIP_2017_FINAL): $(MEMBERSHIP_2017_ZIP2_RENAMED)
	unzip -o $< $(MEMBERSHIP_2017_FILE)
	mv $(MEMBERSHIP_2017_FILE) $@
	touch $@

$(DIRECTORY_2017_ZIP):
	curl -O "$(DIRECTORY_2017_URL)"

$(DIRECTORY_2017_FINAL): $(DIRECTORY_2017_ZIP)
	unzip -o $< $(DIRECTORY_2017_FILE)
	mv $(DIRECTORY_2017_FILE) $@
	touch $@

geo: $(GEO_2017_FINAL)
.PHONY: geo

lunch: $(LUNCH_2017_FINAL)
.PHONY: lunch

characteristics: $(CHARACTERISTICS_2017_FINAL)
.PHONY: characteristics

membership: $(MEMBERSHIP_2017_FINAL)
.PHONY: membership

directory: $(DIRECTORY_2017_FINAL)
.PHONY: directory

download: geo lunch characteristics membership directory
.PHONY: download
