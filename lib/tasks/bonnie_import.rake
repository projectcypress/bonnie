# bundle exec rake bonnie:import:cypress_bundle\[bundle-2023.0.0,bundle2023@mitre.org\]
# bundle exec rake bonnie:import:add_description\[bundle2023@mitre.org\]
# bundle exec rake bonnie:import:sort_entries\[bundle2023@mitre.org\]
# bundle exec rake bonnie:import:limit_measures\[bundle2023@mitre.org\]
# bundle exec rake bonnie:import:add_all_measures\[bundle2023@mitre.org\]
# bundle exec rake bonnie:import:export_vs\[bundle2023@mitre.org\]
# bundle exec rake bonnie:import:delete_patients\[bundle2023@mitre.org\]

namespace :bonnie do
  namespace :import do
    task setup: :environment

    SOURCE_ROOTS = { bundle: 'bundle.json',
                     measures: 'measures', measures_info: 'measures-info.json',
                     calculations: 'calculations',
                     valuesets: File.join('value-sets', 'value-set-codes.csv'),
                     patients: 'patients' }.freeze
    COLLECTION_NAMES = ['bundles', 'records', 'measures', 'individual_results', 'system.js'].freeze
    DEFAULTS = { type: nil,
                 update_measures: true,
                 clear_collections: COLLECTION_NAMES }.freeze

    task :patients_with_vs, [:email, :data_type, :oid] => :setup do |_, args|
      user = User.find_by email: args.email
      vs = CQM::ValueSet.where(group_id: user.current_group.id, oid: args.oid).first
      vs_codes = vs.concepts.collect(&:code)
      CSV.open("tmp/#{args.data_type.gsub('::','_')}_#{args.oid.gsub('.','_')}.csv", 'w', col_sep: '|') do |csv|
        user.current_group.patients.each_with_index do |patient, index|
          found = false
          next if found
          patient.qdmPatient.dataElements.each_with_index do |de, de_index|
            next unless de._type == args.data_type 
            de_codes = de.dataElementCodes.map { |de| de['code'] }
            next if (de_codes & vs_codes).empty?

            puts index
            found = true
            byebug
            csv << [patient.id.to_s, patient.familyName, patient.givenNames.first, de_index, de.description]
          end
        end
      end
    end

    task :delete_patients, [:email] => :setup do |_, args|
      user = User.find_by email: args.email
      user.current_group.patients.destroy_all
    end

    task :hep_b, [:email] => :setup do |_, args|
      user = User.find_by email: args.email
      rx_vs = CQM::ValueSet.where(group_id: user.current_group.id, oid: '2.16.840.1.113883.3.464.1003.196.12.1216').first
      pro_vs = CQM::ValueSet.where(group_id: user.current_group.id, oid: '2.16.840.1.113883.3.464.1003.110.12.1042').first
      vs_codes = rx_vs.concepts.collect(&:code)
      vs_codes.concat(pro_vs.concepts.collect(&:code))
      data_types= ["QDM::ProcedurePerformed", "QDM::ImmunizationAdministered"]
      CSV.open("tmp/hep_b_cypress.csv", 'w', col_sep: '|') do |csv|
        user.current_group.patients.each_with_index do |patient, index|
          found = false
          next if found
          patient.qdmPatient.dataElements.each_with_index do |de, de_index|
            next unless data_types.include?(de._type)
            de_codes = de.dataElementCodes.map { |de| de['code'] }
            next if (de_codes & vs_codes).empty?

            puts index
            found = true
            csv << [patient.id.to_s, patient.familyName, patient.givenNames.first, de_index, de.description, de.relevantDatetime&.to_date, patient.qdmPatient.birthDatetime&.to_date, de.relevantDatetime&.to_date == patient.qdmPatient.birthDatetime&.to_date]
          end
        end
      end
    end

    task :qrda, [:file, :email, :measure] => :setup do |_, args|
      vendor_patient_file = File.new(args.file)
      artifact = Artifact.new(file: vendor_patient_file)
      user = User.find_by email: args.email
      measure = if user.current_group.cqm_measures.where(cms_id: args.measure).first
                  user.current_group.cqm_measures.where(cms_id: args.measure).first
                else
                  user.current_group.cqm_measures.first
                end
      artifact.each do |name, data|
        # Add a patient if it passes CDA validation
        doc = Nokogiri::XML::Document.parse(data)
        doc.root.add_namespace_definition('cda', 'urn:hl7-org:v3')
        doc.root.add_namespace_definition('sdtc', 'urn:hl7-org:sdtc')
        patient, _warnings, codes = QRDA::Cat1::PatientImporter.instance.parse_cat1(doc)
        patient.user = user
        patient.measure_ids << measure.hqmf_set_id
        patient.save
      end
    end

    task :export_vs, [:email] => :setup do |_, args|
      user = User.find_by email: args.email
      vset_ids = CQM::Measure.where(group_id: user.current_group.id).map { |mes| mes.value_set_ids }.flatten.uniq
      already_printed = []

      CSV.open('tmp/value-set-codes.csv', 'a', col_sep: '|') do |csv|
        csv << ['OID', 'ValueSetName', 'ExpansionVersion', 'Code', 'Descriptor', 'CodeSystemName', 'CodeSystemVersion', 'CodeSystemOID', 'Purpose']
        CQM::ValueSet.find(vset_ids).each do |vs|
          next if already_printed.include? (vs.oid)
          next if vs.oid[0,4] == 'drc-'
          already_printed << vs.oid
          # CSV.open('tmp/value-set-codes.csv', 'a', col_sep: '|') do |csv|
          vs.concepts.each do |concept|
            csv << [vs.oid, vs.display_name, vs.version, concept.code, concept.display_name, concept.code_system_name, concept.code_system_version, concept.code_system_oid, ""]
          end
        end
      end
    end

    task :export_csv, [:email] => :setup do |_, args|
      user = User.find_by email: args.email
      value_sets = []
      categorized_codes = {}
      code_vs_count = {}
      code_description_hash = {}
      CQM::ValueSet.where(user_id: user.id).distinct(:oid).each do |oid|
        value_sets << CQM::ValueSet.where(user_id: user.id, oid: oid).first
      end
      #criteria_valuesets = value_sets.select { |vs| vs.concepts.any? { |concept| concept['code'] == data_element['dataElementCodes'][0]['code'] } }
      CSV.open('tmp/patients.csv', 'w', col_sep: '|') do |csv|
        user.current_group.patients.each_with_index do |patient, index|
          puts index
          patient.qdmPatient.dataElements.each do |data_element|
            code = data_element['dataElementCodes'][0]['code']
            unless code_description_hash.key?(code)
              vs = value_sets.select { |vs| vs.concepts.any? { |concept| concept['code'] == code } }.first
              if vs.nil?
                puts code
              else
                code_description_hash[code] = vs.concepts.select { |c| c.code == code }.first.display_name
              end
            end
            negated = (data_element.respond_to?(:negationRationale) && data_element.negationRationale) ? '(not done) ' : ''
            result = (data_element.respond_to?(:result) && data_element.result) ? data_element.result.to_s : ''
            description = data_element['description'] ? negated + data_element['description'] : ''
            if code_vs_count[code].nil?
              code_vs_count[code] = value_sets.select { |vs| vs.concepts.any? { |concept| concept['code'] == code } }.size
            end
            de_time = data_element_time(data_element)
            de_time.strftime('%m/%d/%Y')
            de_time.strftime('%T')
            csv << ["#{patient.familyName} #{patient.givenNames[0]}", data_element['_type'], description, result, de_time.strftime('%m/%d/%Y'), de_time.strftime('%T'), code, code_description_hash[code], code_vs_count[code]]
          end
        end
      end
    end

    task :limit_measures, [:email] => :setup do |_, args|
      user = User.find_by email: args.email
      user.current_group.patients.each_with_index do |patient, index|
        patient.measure_ids = patient.expectedValues.select { |ev| ev['IPP'] > 0 }.map { |ev| ev['measure_id'] }.uniq
        # other_ids = ['703CC49B-B653-4885-80E8-245A057F5AE9', 'C3657D72-21B4-4675-820A-86C7FE293BF5', '38B0B5EC-0F63-466F-8FE3-2CD20DDD1622', 'FA91BA68-1E66-4A23-8EB2-BAA8E6DF2F2F']
        # patient.measure_ids.concat(other_ids)
        patient.save
      end
    end

    task :orphans, [:email] => :setup do |_, args|
      user = User.find_by email: args.email
      user.current_group.patients.each_with_index do |patient, index|
        if patient.measure_ids.compact.empty?
          puts "#{patient.givenNames[0]} #{patient.familyName}"
        end
      end
    end

    task :encounters, [:email] => :setup do |_, args|
      user = User.find_by email: args.email
      user.current_group.patients.each_with_index do |patient, index|
        puts "#{patient.familyName} #{patient.givenNames[0]} - #{patient.qdmPatient.encounters.size}"
      end
    end

    task :add_all_measures, [:email] => :setup do |_, args|
      user = User.find_by email: args.email
      measure_ids = user.current_group.cqm_measures.map(&:hqmf_set_id)
      user.current_group.patients.each_with_index do |patient, index|
        patient.measure_ids = measure_ids
        patient.save
      end
    end

    task :add_description, [:email] => :setup do |_, args|
      user = User.find_by email: args.email
      @code_hash = {}
      valuesets = user.current_group.cqm_measures.collect(&:value_sets).flatten.uniq
      valuesets.each do |vs|
        vs.concepts.each do |concept|
          @code_hash[key_for_code(concept.code, concept.code_system_oid)] = vs.display_name
        end
      end
      CSV.open('tmp/missing_descriptions.csv', 'w', col_sep: '|') do |csv|
        user.current_group.patients.each_with_index do |patient, index|
          puts index
          patient.qdmPatient.dataElements.each do |data_element|
            csv << ["#{patient.familyName} #{patient.givenNames[0]}", data_element._type, data_element&.codes&.first&.code] unless add_description_to_data_element(data_element)
          end
          patient.save
        end
      end
    end

    def key_for_code(code, system)
      Digest::SHA2.hexdigest("#{code} #{system}")
    end

    def add_description_to_data_element(data_element)
      de = data_element
      de.codes.each do |code|
        display_name = @code_hash[key_for_code(code.code, code.system)]
        qdm_status = de.respond_to?(:qdmStatus) ? "#{de.qdmStatus.capitalize}: " : ""
        de.description = qdm_status + display_name if display_name 
      end
      return de.description.nil? ? false : true
    end

    task :sort_entries, [:email] => :setup do |_, args|
      user = User.find_by email: args.email
      user.current_group.patients.each_with_index do |patient, index|
        puts index
        sorted_elements = patient.qdmPatient.dataElements.sort_by! { |de| data_element_time(de) }.clone
        patient.qdmPatient.dataElements.destroy_all
        patient.qdmPatient.dataElements << sorted_elements
        patient.save
      end
    end

    task :shift_entries, [:email] => :setup do |_, args|
      patient = Patient.first
      earlist_jan_date = patient.qdmPatient.dataElements.map { |de| data_element_time(de).day if data_element_time(de).month == 1 && data_element_time(de).year != 2030 }.compact.min
      latese_dec_date = patient.qdmPatient.dataElements.map { |de| data_element_time(de).day if data_element_time(de).month == 12 && data_element_time(de).year != 2030 }.compact.max
      earliest_shift = earlist_jan_date ||= -30
      latest_shift = latese_dec_date ||= 30
      date_shift = 86400 * Random.rand(earliest_shift..latest_shift)
      patient.qdmPatient.shift_dates(date_shift)
      patient.qdmPatient.dataElements.destroy_all
      byebug
    end

    def self.data_element_time(data_element)
      return data_element['relevantPeriod']['low'] if data_element['relevantPeriod'] && data_element['relevantPeriod']['low']
      return data_element['relevantDatetime'] if data_element['relevantDatetime']
      return data_element['prevalencePeriod']['low'] if data_element['prevalencePeriod'] && data_element['prevalencePeriod']['low']
      return data_element['authorDatetime'] if data_element['authorDatetime']
      return data_element['resultDatetime'] if data_element['resultDatetime']
      return data_element['sentDatetime'] if data_element['sentDatetime']
      return data_element['participationPeriod']['low'] if data_element['participationPeriod'] && data_element['participationPeriod']['low']
      return data_element['birthDatetime'] if data_element['birthDatetime']
      return data_element['expiredDatetime'] if data_element['expiredDatetime']
      return Date.new(2030,1,1) #if ['QDM::PatientCharacteristicEthnicity', 'QDM::PatientCharacteristicRace', 'QDM::PatientCharacteristicSex'].include?(data_element._type)
    end

    task :all_links, [:email] => :setup do |_, args|
      user = User.find_by email: args.email
      user.current_group.cqm_measures.each do |measure|
        puts "http://localhost:3000/#measures/#{measure.hqmf_set_id.upcase}"
      end
    end

    task :cypress_bundle, [:file, :email] => :setup do |_, args|
      user = User.find_by email: args.email
      user.current_group.cqm_measures.each do |measure|
        measure.measure_period['low']['value'] = '202201010000'
        measure.measure_period['high']['value'] = '202212312359'
        measure.save
      end
      categorized_codes = get_categorized_codes(user)
      measure_ids = user.current_group.cqm_measures.map(&:hqmf_set_id)
      Zip::ZipFile.open(args.file) do |zip_file|
        unpack_and_store_cqm_patients(zip_file, user.current_group, measure_ids, categorized_codes)
        unpack_and_store_calcuations(zip_file, user)
      end
    end

    task :remove_unmatched, [:email] => :setup do |_, args|
      user = User.find_by email: args.email
      categorized_codes = get_categorized_codes(user)
      user.current_group.patients.each_with_index do |patient, index|
        remove_unmatched_data_type_code_combinations(patient, categorized_codes)
      end
    end

    def self.unpack_and_store_cqm_patients(zip, current_group, measure_ids, categorized_codes)
      qrda_files = zip.glob(File.join(SOURCE_ROOTS[:patients], '**', '*.xml'))
      qrda_files.each_with_index do |qrda_file, index|
        qrda = qrda_file.get_input_stream.read
        doc = Nokogiri::XML::Document.parse(qrda)
        doc.root.add_namespace_definition('cda', 'urn:hl7-org:v3')
        doc.root.add_namespace_definition('sdtc', 'urn:hl7-org:sdtc')
        patient, _warnings, codes = QRDA::Cat1::PatientImporter.instance.parse_cat1(doc)

        patient.group = current_group
        patient.expectedValues = []
        patient.measure_ids.concat(measure_ids)
        remove_unmatched_data_type_code_combinations(patient, categorized_codes)
        patient.save
      end
    end

    def self.get_categorized_codes(user)
      value_sets = []
      categorized_codes = {}
      CQM::ValueSet.where(group_id: user.current_group.id).distinct(:oid).each do |oid|
        value_sets << CQM::ValueSet.where(group_id: user.current_group.id, oid: oid).first
      end
      %w[medication substance].each do |qdm_category|
        data_criteria = user.current_group.cqm_measures.collect { |m| m.source_data_criteria.select { |sdc| sdc.qdmCategory == qdm_category } }.flatten
        criteria_valuesets = value_sets.select { |vs| data_criteria.collect(&:codeListId).include?(vs.oid) }
        code_list = criteria_valuesets.collect(&:concepts).flatten
        categorized_codes[qdm_category] = code_list.map { |cl| { 'code' => cl.code, 'system' => cl.code_system_oid } }
      end
      categorized_codes
    end

    def self.remove_unmatched_data_type_code_combinations(patient, categorized_codes)
      de_to_delete = []
      categorized_codes.each do |qdm_category, codes|
        de_to_delete += patient.qdmPatient.get_data_elements(qdm_category).map { |de| de unless data_element_has_appropriate_codes(de, codes) }
      end
      de_to_delete.compact.each(&:destroy)
    end

    def self.data_element_has_appropriate_codes(data_element, codes)
      !(data_element.dataElementCodes.map { |dec| { 'code' => dec[:code], 'system' => dec[:system] } } & codes).flatten.empty?
    end

    def self.unpack_and_store_calcuations(zip, user)
      patient_id_mapping = {}
      measure_id_mapping = {}
      patient_id_csv = CSV.parse(zip.read(File.join(SOURCE_ROOTS[:calculations], 'patient-id-mapping.csv')), headers: false)
      measure_id_csv = CSV.parse(zip.read(File.join(SOURCE_ROOTS[:calculations], 'measure-id-mapping.csv')), headers: false)
      patient_id_csv.each do |row|
        patient = user.current_group.patients.where(givenNames: [row[1]], familyName: row[2]).first
        next unless patient

        patient_id_mapping[row[0]] = { givenNames: row[1],
                                       familyName: row[2],
                                       new_id: patient.id }
      end
      measure_id_csv.each do |row|
        measure = user.current_group.cqm_measures.where(cms_id: row[1]).first
        next unless measure
        measure_id_mapping[row[0]] = { cms_id: row[1], new_id: measure.id }
      end
      unpack_and_store_individual_results(zip, patient_id_mapping, measure_id_mapping)
      true
    rescue => e
      byebug
      false
    end

    def self.unpack_and_store_individual_results(zip, patient_id_mapping, measure_id_mapping)
      individual_result_files = zip.glob(File.join(SOURCE_ROOTS[:calculations], 'individual-results', '*.json'))
      total_count = individual_result_files.size
      individual_result_files.each_with_index do |ir_file, index|
        ir = JSON.parse(ir_file.get_input_stream.read)
        # new_ir = CQM::IndividualResult.new(ir)
        # new_ir.correlation_id = bundle.id.to_s
        next unless patient_id_mapping[ir['patient_id']]
        patient_id = patient_id_mapping[ir['patient_id']][:new_id]
        next unless measure_id_mapping[ir['measure_id']]
        measure_id = measure_id_mapping[ir['measure_id']][:new_id]
        measure = CQM::Measure.find(measure_id)
        patient = CQM::Patient.find(patient_id)
        population_hash = measure.population_set_hash_for_key(ir['population_set_key'])
        pop_index = measure.population_sets_and_stratifications_for_measure.index(population_hash)
        expectedValue = { 'population_index' => pop_index, 'measure_id' => measure.hqmf_set_id }
        measure.population_keys.each do |pop|
          expectedValue[pop] = ir[pop]
        end
        if population_hash && population_hash[:stratification_id]
          expectedValue['STRAT'] = 1
        end
        patient.expectedValues << expectedValue
        patient.save
      end
    end
  end
end
