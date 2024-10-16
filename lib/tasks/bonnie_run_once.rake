require 'colorize'
require_relative '../util/cql_to_elm_helper'

# This rakefile is for tasks that are designed to be run once to address a specific problem; we're keeping
# them as a history and as a reference for solving related problems

namespace :bonnie do
  namespace :patients do
    desc %(Download excel exports for all measures specified in ACCOUNTS
    $ rake bonnie:patients:super_user_excel)
    task :super_user_excel => :environment do
      # To run, put a space dilimeted list of emails from which you would like to download the excel exports from
      ACCOUNTS = %w().freeze
      ACCOUNTS.each do |account|
        user_measures = CQM::Measure.by_user(User.find_by(email: account))
        user_measures.each do |measure|
          begin
            user = User.find(measure.user_id)
            hqmf_set_id = measure.hqmf_set_id
            @api_v1_patients = CQM::Patient.by_user_and_hqmf_set_id(user, measure.hqmf_set_id)
            @calculator_options = { doPretty: true }

            calculated_results = BonnieBackendCalculator.calculate(measure, @api_v1_patients, @calculator_options)
            converted_results = ExcelExportHelper.convert_results_for_excel_export(calculated_results, measure, @api_v1_patients)
            patient_details = ExcelExportHelper.get_patient_details(@api_v1_patients)
            population_details = ExcelExportHelper.get_population_details_from_measure(measure, calculated_results)
            statement_details = ExcelExportHelper.get_statement_details_from_measure(measure)

            filename = "#{user._id}-#{measure.hqmf_set_id}.xlsx"
            excel_package = PatientExport.export_excel_cql_file(converted_results, patient_details, population_details, statement_details, measure.hqmf_set_id)

            path = File.join Rails.root, 'exports'
            FileUtils.mkdir_p(path) unless File.exist?(path)
            File.open(File.join(path, filename), "wb") do |f|
              f.write(excel_package.to_stream.read)
            end
          rescue Exception => e
            puts measure.cms_id
            puts user.email
            puts e
          end
        end
      end
    end

    desc %(Update source_data_criteria to match fields from measure
    $ rake bonnie:patients:update_source_data_criteria)
    task :update_source_data_criteria=> :environment do
      puts "Updating patient source_data_criteria to match measure"
      p_hqmf_set_ids_updated = 0
      hqmf_set_ids_updated = 0
      p_code_list_ids_updated = 0
      code_list_ids_updated = 0
      successes = 0
      warnings = 0
      errors = 0

      CQM::Patient.all.each do |patient|
        p_hqmf_set_ids_updated = 0

        first = patient.first
        last = patient.last

        begin
          email = User.find_by(_id: patient[:user_id]).email
        rescue Mongoid::Errors::DocumentNotFound
          print_error("#{first} #{last} #{patient[:user_id]} Unable to find user")
        end

        has_changed = false
        hqmf_set_id = patient.measure_ids[0]

        begin
          measure = CQM::Measure.find_by(hqmf_set_id: patient.measure_ids[0], user_id: patient[:user_id])
        rescue Mongoid::Errors::DocumentNotFound => e
          print_warning("#{first} #{last} #{email} Unable to find measure")
          warnings += 1
        end

        patient.source_data_criteria.each do |patient_data_criteria|
          if patient_data_criteria['hqmf_set_id'] && patient_data_criteria['hqmf_set_id'] != hqmf_set_id
            patient_data_criteria['hqmf_set_id'] = hqmf_set_id
            p_hqmf_set_ids_updated += 1
            has_changed = true
          end

          if patient_data_criteria['code_list_id'] && patient_data_criteria['code_list_id'].include?('-')
            # Extract the correct guid from the measure
            unless measure.nil?
              patient_criteria_updated = false
              patient_criteria_matches = false
              measure.source_data_criteria.each do |measure_id, measure_criteria|
                if measure_id == patient_data_criteria['id']
                  if patient_data_criteria['code_list_id'] != measure_criteria['code_list_id']
                    patient_data_criteria['code_list_id'] = measure_criteria['code_list_id']
                    has_changed = true
                    p_code_list_ids_updated += 1
                    patient_criteria_updated = true
                  else
                    patient_criteria_matches = true
                  end
                else
                  # some ids have inconsistent guids for some reason, but the prefix part still
                  # allows for a mapping.
                  get_id_head = lambda do |id|
                    # handle special case with one weird measure
                    if id == 'HBsAg_LaboratoryTestPerformed_40280381_3d61_56a7_013e_7f3878ec7630_source' ||
                       id == 'prefix_5195_3_LaboratoryTestPerformed_2162F856_8C15_499E_AC82_E58B05D4B568_source'
                      id = 'prefix_5195_3_LaboratoryTestPerformed'
                    else
                      # remove the guid and _source from the id
                      id = id[0..id.index(/(_[a-zA-Z0-9]*){5}_source/)]
                    end
                  end
                  patient_id_head = get_id_head.call(patient_data_criteria['id'])
                  measure_id_head = get_id_head.call(measure_id)
                  if patient_id_head == measure_id_head
                    if patient_data_criteria['code_list_id'] != measure_criteria['code_list_id']
                      patient_data_criteria['code_list_id'] = measure_criteria['code_list_id']
                      has_changed = true
                      p_code_list_ids_updated += 1
                      patient_criteria_updated = true
                    else
                      patient_criteria_matches = true
                    end
                  end
                end
              end
              if !patient_criteria_updated && !patient_criteria_matches
                # print an error if we have looked at all measure_criteria but still haven't found a match.
                print_error("#{first} #{last} #{email} Unable to find code list id for #{patient_data_criteria['title']}")
                errors += 1
              end
            end
          end
        end

        begin
          patient.save!
          if has_changed
            hqmf_set_ids_updated += p_hqmf_set_ids_updated
            code_list_ids_updated += p_code_list_ids_updated
            successes += 1
            print_success("Fixing mismatch on User: #{email}, first: #{first}, last: #{last}")
          end
        rescue Mongo::Error::OperationFailure => e
          errors += 1
          print_error("#{e}: #{email}, first: #{first}, last: #{last}")
        end
      end
      puts "Results".center(80, '-')
      puts "Patients successfully updated: #{successes}"
      puts "Errors: #{errors}"
      puts "Warnings: #{warnings}"
      puts "Hqmf_set_ids Updated: #{hqmf_set_ids_updated}"
      puts "Code_list_ids Updated: #{code_list_ids_updated}"
    end

    desc "Garbage collect/fix expected_values structures"
    task :expected_values_garbage_collect => :environment do
      # build structures for holding counts of changes
      patient_values_changed_count = 0
      total_patients_count = 0
      user_counts = []
      puts "Garbage collecting/fixing expected_values structures"
      puts ""

      # loop through users
      User.asc(:email).all.each do |user|
        user_count = {email: user.email, total_patients_count: 0, patient_values_changed_count: 0, measure_counts: []}

        # loop through measures
        CQM::Measure.by_user(user).each do |measure|
          measure_count = {cms_id: measure.cms_id, title: measure.title, total_patients_count: 0, patient_values_changed_count: 0}

          # loop through each patient in the measure
          CQM::Patient.by_user_and_hqmf_set_id(user, measure.hqmf_set_id).each do |patient|
            user_count[:total_patients_count] += 1
            measure_count[:total_patients_count] += 1
            total_patients_count += 1

            # do the updating of the structure
            items_changed = false
            patient.update_expected_value_structure!(measure) do |change_type, change_reason, expected_value_set|
              puts "#{user.email} - #{measure.cms_id} - #{measure.title} - #{patient.givenNames[0]} #{patient.familyName} - #{change_type} because #{change_reason}"
              pp(expected_value_set)
              items_changed = true
            end

            # if anything was removed print the final structure
            if items_changed
              measure_count[:patient_values_changed_count] += 1
              user_count[:patient_values_changed_count] += 1
              patient_values_changed_count += 1
              puts "#{user.email} - #{measure.cms_id} - #{measure.title} - #{patient.givenNames[0]} #{patient.familyName} - FINAL STRUCTURE:"
              pp(patient.qdmPatient.expectedValues)
              puts ""
            end
          end

          user_count[:measure_counts] << measure_count

        end
        user_counts << user_count
      end

      puts "--- Complete! ---"
      puts ""

      if patient_values_changed_count > 0
        puts "\e[31mexpected_values changed on #{patient_values_changed_count} of #{total_patients_count} patients\e[0m"
        user_counts.each do |user_count|
          if user_count[:patient_values_changed_count] > 0
            puts "#{user_count[:email]} - #{user_count[:patient_values_changed_count]}/#{user_count[:total_patients_count]}"
            user_count[:measure_counts].each do |measure_count|
              print "\e[31m" if measure_count[:patient_values_changed_count] > 0
              puts "  #{measure_count[:patient_values_changed_count]}/#{measure_count[:total_patients_count]} on #{measure_count[:cms_id]} - #{measure_count[:title]}\e[0m"
            end
          end
        end
      else
        puts "\e[32mNo expected_values changed\e[0m"
      end
    end

    def old_unit_to_ucum_unit(unit)
      case unit
        when 'capsule(s)'
          '{Capsule}'
        when 'dose(s)'
          '{Dose}'
        when 'gram(s)'
          'g'
        when 'ml(s)'
          'mL'
        when 'tablet(s)'
          '{tbl}'
        when 'mcg(s)'
          'ug'
        when 'unit(s)'
          '{unit}'
        else
          unit
      end
    end

    desc %(Recreates the JSON elm stored on CQL measures using an instance of
      a locally running CQLTranslationService JAR and updates the code_list_id field on
      data_criteria and source_data_criteria for direct reference codes. This is in run_once
      because once all of the code_list_ids have been updated to use a hash of the parameters
      in direct reference codes, all code_list_ids for direct reference codes measures uploaded
      subsequently will be correct
    $ rake bonnie:patients:rebuild_elm_update_drc_code_list_ids)
    task :rebuild_elm_update_drc_code_list_ids => :environment do
      update_passes = 0
      update_fails = 0
      orphans = 0
      fields_diffs = Hash.new(0)
      CQM::Measure.all.each do |measure|
        begin
          # Grab the user, we need this to output the name of the user who owns
          # this measure. Also comes in handy when detecting measures uploaded
          # by accounts that have since been deleted.
          user = User.find_by(_id: measure[:user_id])
          cql = nil
          cql_artifacts = nil
          # Grab the name of the main cql library
          main_cql_library = measure[:main_cql_library]

          # Grab a copy of all attributes that we will update the measure with.
          before_state = {}
          before_state[:measure_data_criteria] = measure[:data_criteria].deep_dup
          before_state[:measure_source_data_criteria] = measure[:source_data_criteria].deep_dup
          before_state[:measure_cql] = measure[:cql].deep_dup
          before_state[:measure_elm] = measure[:elm].deep_dup
          before_state[:measure_elm_annotations] = measure[:elm_annotations].deep_dup
          before_state[:measure_cql_statement_dependencies] = measure[:cql_statement_dependencies].deep_dup
          before_state[:measure_main_cql_library] = measure[:main_cql_library].deep_dup
          before_state[:measure_value_set_oids] = measure[:value_set_oids].deep_dup
          before_state[:measure_value_set_oid_version_objects] = measure[:value_set_oid_version_objects].deep_dup

          # Grab the existing data_criteria and source_data_criteria hashes. Must be a deep copy, due to how Mongo copies Hash and Array field types.
          data_criteria_object = {}
          data_criteria_object['data_criteria'] = measure[:data_criteria].deep_dup
          data_criteria_object['source_data_criteria'] = measure[:source_data_criteria].deep_dup

          # If measure has been uploaded more recently (we should have a copy of the MAT Package) we will use the actual MAT artifacts
          if !measure.package.nil?
            # Create a temporary directory
            Dir.mktmpdir do |dir|
              # Write the package to a temp directory
              File.open(File.join(dir, measure.measure_id + '.zip'), 'wb') do |zip_file|
                # Write the package binary to a zip file.
                zip_file.write(measure.package.file.data)
                files = Measures::CqlLoader.get_files_from_zip(zip_file, dir)
                cql_artifacts = Measures::CqlLoader.process_cql(files, main_cql_library, user, nil, nil, measure.hqmf_set_id)
                updated_data_criteria_object = set_data_criteria_code_list_ids(data_criteria_object, cql_artifacts)
                data_criteria_object['source_data_criteria'] = updated_data_criteria_object[:source_data_criteria]
                data_criteria_object['data_criteria'] = updated_data_criteria_object[:data_criteria]
                cql = files[:CQL]
              end
            end
          # If the measure does not have a MAT package stored, continue as we have in the past using the cql to elm service
          else
            # Grab the measure cql
            cql = measure[:cql]
            # Use the CQL-TO-ELM Translation Service to regenerate elm for older measures.
            elm_json, elm_xml = CqlToElmHelper.translate_cql_to_elm(cql)
            elms = {:ELM_JSON => elm_json,
                    :ELM_XML => elm_xml}
            cql_artifacts = Measures::CqlLoader.process_cql(elms, main_cql_library, user, nil, nil, measure.hqmf_set_id)
            updated_data_criteria_object = set_data_criteria_code_list_ids(data_criteria_object, cql_artifacts)
            data_criteria_object['source_data_criteria'] = updated_data_criteria_object[:source_data_criteria]
            data_criteria_object['data_criteria'] = updated_data_criteria_object[:data_criteria]
          end

          # Get a hash of differences from the original measure and the updated data
          differences = measure_update_diff(before_state, data_criteria_object, cql, cql_artifacts, main_cql_library)
          unless differences.empty?
            # Remove value set oids that don't start with 'drc-' but do contain '-'
            updated_value_set_oid_version_objects = cql_artifacts[:value_set_oid_version_objects].find_all do |oid_version_object|
              !oid_version_object[:oid].include?('drc-') && oid_version_object[:oid].include?('-') ? false : true
            end
            # Remove value set oids that don't start with 'drc-' but do contain '-'
            updated_value_set_oids = cql_artifacts[:all_value_set_oids].find_all do |oid|
              !oid.include?('drc-') && oid.include?('-') ? false : true
            end

            # Update the measure
            measure.update(data_criteria: data_criteria_object['data_criteria'], source_data_criteria: data_criteria_object['source_data_criteria'], cql: cql, elm: cql_artifacts[:elms], elm_annotations: cql_artifacts[:elm_annotations], cql_statement_dependencies: cql_artifacts[:cql_definition_dependency_structure],
                           main_cql_library: main_cql_library, value_set_oids: updated_value_set_oids, value_set_oid_version_objects: updated_value_set_oid_version_objects)
            measure.save!
            update_passes += 1
            print "\e[#{32}m#{"[Success]"}\e[0m"
            puts ' Measure ' + "\e[1m#{measure[:cms_id]}\e[22m" + ': "' + measure[:title] + '" with id ' + "\e[1m#{measure[:id]}\e[22m" + ' in account ' + "\e[1m#{user[:email]}\e[22m" + ' successfully updated ELM!'
            differences.each_key do |key|
              fields_diffs[key] += 1
              puts "--- #{key} --- Has been modified"
            end
          end
        rescue Mongoid::Errors::DocumentNotFound => e
          orphans += 1
          print "\e[#{31}m#{"[Error]"}\e[0m"
          puts ' Measure ' + "\e[1m#{measure[:cms_id]}\e[22m" + ': "' + measure[:title] + '" with id ' + "\e[1m#{measure[:id]}\e[22m" + ' belongs to a user that doesn\'t exist!'
        rescue Exception => e
          update_fails += 1
          print "\e[#{31}m#{"[Error]"}\e[0m"
          puts ' Measure ' + "\e[1m#{measure[:cms_id]}\e[22m" + ': "' + measure[:title] + '" with id ' + "\e[1m#{measure[:id]}\e[22m" + ' in account ' + "\e[1m#{user[:email]}\e[22m" + ' failed to update ELM!'
        end
      end
      puts "#{update_passes} measures successfully updated."
      puts "#{update_fails} measures failed to update."
      puts "#{orphans} measures are orphaned, and were not updated."
      puts "Overall number of fields changed."
      fields_diffs.each do |key, value|
        puts "-- #{key}: #{value} --"
      end
    end

    def self.set_data_criteria_code_list_ids(json, cql_artifacts)
      # Loop over data criteria to search for data criteria that is using a single reference code.
      # Once found set the Data Criteria's 'code_list_id' to our fake oid. Do the same for source data criteria.
      json['data_criteria'].each do |data_criteria_name, data_criteria|
        # We do not want to replace an existing code_list_id. Skip it, unless it is a GUID.
        if !data_criteria.key?('code_list_id') || (data_criteria['code_list_id'] && data_criteria['code_list_id'].include?('-'))
          if data_criteria['inline_code_list']
            # Check to see if inline_code_list contains the correct code_system and code for a direct reference code.
            data_criteria['inline_code_list'].each do |code_system, code_list|
              # Loop over all single code reference objects.
              cql_artifacts[:single_code_references].each do |single_code_object|
                # If Data Criteria contains a matching code system, check if the correct code exists in the data critera values.
                # If both values match, set the Data Criteria's 'code_list_id' to the single_code_object_guid.
                if code_system == single_code_object[:code_system_name] && code_list.include?(single_code_object[:code])
                  data_criteria['code_list_id'] = single_code_object[:guid]
                  # Modify the matching source data criteria
                  json['source_data_criteria'][data_criteria_name + "_source"]['code_list_id'] = single_code_object[:guid]
                end
              end
            end
          end
        end
      end
      {source_data_criteria: json['source_data_criteria'], data_criteria: json['data_criteria']}
    end

    namespace :cypress do
      # bundle exec rake bonnie:cypress:associate_measures EMAIL='raketest@gmail.com'
      desc "Associate each patient with every measure for a specific user (identified by email address)"
      task :associate_measures => :environment do
        user = User.where(email: ENV['EMAIL']).first
        measures = CQM::Measure.where(user_id: user.id)
        all_measure_ids = measures.map(&:hqmf_set_id) # array of all measure_ids (string) for patient
        user.records.each do |patient|
          # note: this associates *every* patient with every measure,
          # so any orphaned patients (patients on a measure that has been deleted) will come back
          patient.measure_ids = all_measure_ids
          # add null entry to end of measure_ids array to match existing bonnie records
          patient.measure_ids << nil
          patient.save
        end
      end
    end

  end

  desc %{Converts Bonnie patients from 5.4 to 5.5 cqm-models
    user email is optional and can be passed in by EMAIL
    If no email is provided, rake task will run on all measures
  $ rake bonnie:convert_patients_qdm_5_4_to_5_5 EMAIL=xxx}
  task :convert_patients_qdm_5_4_to_5_5 => :environment do
    user = User.find_by email: ENV['EMAIL'] if ENV['EMAIL']
    raise StandardError.new("Could not find user #{ENV["EMAIL"]}.") if ENV["EMAIL"] && user.nil?
    bonnie_patients = user ? Mongoid.default_client.database['cqm_patients'].find(user_id: user._id) : Mongoid.default_client.database['cqm_patients'].find()
    count = 0
    puts "Total patients in account: #{bonnie_patients.count}"
    ignored_fields = ['_id', 'qdmVersion', 'qdmTitle', 'hqmfOid', 'qdmCategory', 'qdmStatus']
    bonnie_patients.each do |bonnie_patient|
      begin
        print ".".green
        new_data_elements = []
        diff = []
        updated_qdm_patient = bonnie_patient['qdmPatient']
        updated_qdm_patient['qdmVersion'] = "5.5"
        updated_qdm_patient['dataElements'].each do |element|
          begin
            type_fields = Object.const_get(element['_type']).fields.keys - ignored_fields
            # Remove the transfer of sender and recipient if it is a CommunicationPerformed
            # dataElement. This is because sender and recipient changed from a QDM::Code datatype
            # to a QDM::Entity datatype
            type_fields -= %w{sender recipient} if element['_type'] == 'QDM::CommunicationPerformed'

            diagnoses = []
            # element is treated as a 5.5 model so principalDiagnosis does not return on the element
            # but still exists in the attributes. That is why respond_to? doesn't work
            diagnoses << QDM::DiagnosisComponent.new(code: element['principalDiagnosis'], rank: 1) if element['principalDiagnosis'].present?

            if element['diagnoses'].present?
              element['diagnoses'].each do |diagnosis|
                diagnoses << QDM::DiagnosisComponent.new(code: diagnosis)
              end
            end

            relateds = []
            if element['relatedTo'].present?
              element['relatedTo'].each do |related|
                relateds << related['value']
              end
            end

            (element.keys - type_fields).each do |ignored_field|
              element.delete(ignored_field)
            end
            new_data_element = Object.const_get(element['_type']).new(element)
            new_data_element.id = element['id']['value'] if element['id'].present?
            new_data_element.diagnoses = diagnoses unless diagnoses.empty?
            new_data_element.relatedTo = relateds unless relateds.empty?
            new_data_element.relevantDatetime = element['relevantPeriod']['low'] if element['_type'] == 'QDM::AdverseEvent' && element['relevantPeriod'].present?
            diff << validate_patient_data(element, new_data_element)
            new_data_elements << new_data_element
          rescue Exception => e
            puts e
            puts e.backtrace
            puts element
          end
        end

        diff.each do |element_diff|
          unless element_diff.empty?
            user = User.find_by _id: bonnie_patient['user_id']
            puts "\nConversion Difference".yellow
            puts "Patient #{bonnie_patient['givenNames'][0]} #{bonnie_patient['familyName']} with id #{bonnie_patient['_id']} in account #{user.email}".yellow
            element_diff.each_entry do |element|
              case element
              when 'sender', 'recipient'
                puts "--- #{element} --- Is different from CQL Record, this is expected because sender & recipient were type QDM::Code in 5.4 and are now type QDM::Entity in 5.5, so data is lost".light_blue
              when 'diagnoses'
                puts "--- #{element} --- Is different from CQL Record, this may be caused by the fact that there was a PrincipalDiagnosis on the element as well that got put in diagnoses with a rank of 1. This is expected".light_yellow
              else
                puts "--- #{element} --- Is different from CQL Record, this is unexpected".light_red
              end
            end
          end
        end
        # Remove all old data elements from the patient
        updated_qdm_patient['dataElements'] = []
        # Add all new data elements to the patient
        new_data_elements.each { |item| updated_qdm_patient['dataElements'] << item.as_document }
        begin
          Mongoid.default_client.database['cqm_patients'].update_one({_id: bonnie_patient['_id']}, bonnie_patient)
        rescue Mongo::Error => e
          user = User.find_by _id: bonnie_patient['user_id']
          print_error("#{e}: #{user.email}, first: #{user.givenNames[0]}, last: #{user.familyName}")
        end
        count += 1
      rescue ExecJS::ProgramError, StandardError => e
        # if there was a conversion failure we should record the resulting failure message with the hds model in a
        # separate collection to return
        user = User.find_by _id: bonnie_patient['user_id']
        if bonnie_patient['measure_ids'].first.nil?
          puts "#{user.email}\n  Measure: N/A\n  Patient: #{bonnie_patient['_id']}\n  Conversion failed with message: #{e.message}".light_red
        elsif CQM::Measure.where(hqmf_set_id: bonnie_patient['measure_ids'].first, user_id: bonnie_patient['user_id']).first.nil?
          puts "#{user.email}\n  Measure (hqmf_set_id): #{bonnie_patient['measure_ids'].first}\n  Patient: #{bonnie_patient['_id']}\n  Conversion failed with message: #{e.message}".light_red
        else
          measure = CQM::Measure.where(hqmf_set_id: bonnie_patient['measure_ids'].first, user_id: bonnie_patient['user_id']).first
          puts "#{user.email}\n  Measure: #{measure.title} #{measure.cms_id}\n  Patient: #{bonnie_patient['_id']}\n  Conversion failed with message: #{e.message}".light_red
        end
      end
    end
    puts count
  end

  def validate_patient_data(old_data_element, new_data_element)
    ignored_fields = ['_id', 'qdmVersion', 'hqmfOid']
    differences = []
    (old_data_element.keys - ignored_fields).each do |key|
      if key == 'id'
        differences.push(key) if old_data_element[key]['value'] != new_data_element[key]
      elsif key == 'relatedTo' && old_data_element[key].present?
        old_data_element['relatedTo'].each_with_index do |related, index|
          differences.push(key) if related['value'] != new_data_element['relatedTo'][index]
        end
      elsif key == 'diagnoses' && old_data_element[key].present?
        old_data_element['diagnoses'].each_with_index do |original_diagnosis, index|
          differences.push(key) if original_diagnosis['code'] != new_data_element.diagnoses[index]['code'][:code]
        end
      elsif key == 'relevantDatetime' && old_data_element['relevantPeriod'].present? && old_data_element['_type'] == 'QDM::AdverseEvent'
        differences.push(key) if old_data_element['relevantPeriod']['low'] != new_data_element.relevantDatetime
      elsif old_data_element[key] != new_data_element[key]
        begin
          # Check to see if symbolizing the keys was all that was necessary to make the values equal
          differences.push(key) if old_data_element[key].symbolize_keys != new_data_element[key]
        rescue StandardError => e
          differences.push(key)
        end
      end
    end
    differences
  end

  task :update_value_set_versions => :environment do
    User.all.each do |user|
      puts "Updating value sets for user " + user.email
      begin
        measures = CQM::Measure.where(user_id: user.id)

        measures.each do |measure|
          elms = measure.elm

          Measures::CqlLoader.modify_value_set_versions(elms)

          elms.each do |elm|

            if elm['library'] && elm['library']['valueSets'] && elm['library']['valueSets']['def']
              elm['library']['valueSets']['def'].each do |value_set|
                db_value_sets = HealthDataStandards::SVS::ValueSet.where(user_id: user.id, oid: value_set['id'])

                db_value_sets.each do |db_value_set|
                  if value_set['version'] && db_value_set.version == "N/A"
                    puts "Setting " + db_value_set.version.to_s + " to " + value_set['version'].to_s
                    db_value_set.version = value_set['version']
                    db_value_set.save()
                  end
                end
              end
            end
          end
        end
      rescue Mongoid::Errors::DocumentNotFound => e
        puts "\nNo CQL measures found for the user below"
        puts user.email
        puts user.id
      end
    end
  end

  def compare_excel_spreadsheets(first_spreadsheet_file, second_spreadsheet_file)
    # Verify the sheet titles are the same
    if first_spreadsheet_file.sheets != second_spreadsheet_file.sheets
      puts "   The two Excel files do not have the same number of sheets!".red
      return false
    end

    first_spreadsheet_file.sheets.each do |population_set|
      next if population_set == "KEY"
      first_sheet = first_spreadsheet_file.sheet(population_set)
      second_sheet = second_spreadsheet_file.sheet(population_set)

      # Column headers should be the same
      first_patient_rows = []
      second_patient_rows = []

      # check header separately, always the 3rd row
      if first_sheet.row(2) != second_sheet.row(2)
        puts "   The two Excel files have different columns!".red
        return false
      end

      no_patients1 = first_sheet.cell(1,1) == "Measure has no patients, please re-export with patients"
      no_patients2 = second_sheet.cell(1,1) == "Measure has no patients, please re-export with patients"
      if no_patients1 == no_patients2 && no_patients1
        puts "   Both Excel files have no patients.".green
        return true
      elsif no_patients1 != no_patients2
        puts "   One Excel file has patients and the other does not!".red
        return false
      end

      # sort patients because they are in different orders
      first_patient_row_index = 3

      # find which columns contain first and last names (this can change depending on the population)
      last_name_column_index = first_sheet.row(2).find_index('last')
      first_name_column_index = first_sheet.row(2).find_index('first')
      # add an extra 'column' which uses full name to make the sorting key more unique
      row_count = 0
      first_sheet.each do |first_row|
        row_count += 1
        next if row_count < first_patient_row_index
        # skip patients who have no name (BONNIE-1612)
        next if first_row[first_name_column_index].nil? || first_row[last_name_column_index].nil?
        first_row.push(first_row[first_name_column_index] + first_row[last_name_column_index])
        first_patient_rows.push(first_row)
      end

      row_count = 0
      second_sheet.each do |second_row|
        row_count += 1
        next if row_count < first_patient_row_index
        # skip patients who have no name (BONNIE-1612)
        next if second_row[first_name_column_index].nil? || second_row[last_name_column_index].nil?
        second_row.push(second_row[first_name_column_index] + second_row[last_name_column_index])
        second_patient_rows.push(second_row)
      end

      if first_patient_rows.length != second_patient_rows.length
        puts "   The two Excel files have different number of rows!".red
        return false
      end

      # sort the patients by our generated key, which is now the last element in the row
      sorted_first_rows = first_patient_rows.sort_by { |a| a[-1] }
      sorted_second_rows = second_patient_rows.sort_by { |a| a[-1] }

      if sorted_first_rows != sorted_second_rows
        puts "   The two Excel files do not match!".red
        open('FIRST-diffs', 'a') do |f1|
          open('SECOND-diffs', 'a') do |f2|
            f1.puts '--------------------------------------------'
            f2.puts '--------------------------------------------'
            f1.puts first_spreadsheet_file.instance_variable_get(:@filename)
            f2.puts second_spreadsheet_file.instance_variable_get(:@filename)
            (0...sorted_first_rows.length).each do |idx|
              if sorted_first_rows[idx] != sorted_second_rows[idx]
                f1.puts sorted_first_rows[idx].to_s
                f2.puts sorted_second_rows[idx].to_s
              end
            end
          end
        end
        puts "   Differences written to FIRST-diffs and SECOND-diffs."
        return false
      end
    end
    puts "   Excel files match.".green
    true
  end

  desc %{Compare two directories of Excel exports.
    You must specify the FIRST and SECOND directories of Excel files. Differences found are written
    out to files FIRST-diffs and SECOND-diffs for comparison in a diff tool.
    $ rake bonnie:compare_excel_exports FIRST=path_to_first_files SECOND=path_to_second_files}
  task :compare_excel_exports => :environment do
    first_folder = ENV['FIRST']
    second_folder = ENV['SECOND']
    if first_folder.nil? || second_folder.nil?
      puts "Requires FIRST and SECOND parameters to specify the two folders for comparison!".red
      exit
    end
    File.delete('FIRST-diffs') if File.exist?('FIRST-diffs')
    File.delete('SECOND-diffs') if File.exist?('SECOND-diffs')
    first_folder += "/" unless first_folder.ends_with? "/"
    second_folder += "/" unless second_folder.ends_with? "/"
    Dir.foreach(first_folder) do |file_name|
      next if file_name == '.' || file_name == '..' || !(file_name.end_with? 'xlsx') || (file_name.start_with? '~')
      puts "Comparing " + file_name
      first_excel_file = first_folder + file_name
      second_excel_file = second_folder + file_name
      first_spreadsheet_file = Roo::Spreadsheet.open(first_excel_file)
      unless File.file?(second_excel_file)
        puts "   Corresponding file in SECOND directory does not exist!".red
        next
      end
      second_spreadsheet_file = Roo::Spreadsheet.open(second_excel_file)
      compare_excel_spreadsheets(first_spreadsheet_file, second_spreadsheet_file)
    end
  end

  desc %{Compare two Excel files.
    You must specify the FIRST and SECOND file names. Differences found are written
    out to files FIRST-diffs and SECOND-diffs for comparison in a diff tool.
    $ rake bonnie:compare_excel_files FIRST=filename SECOND=filename}
  task :compare_excel_files => :environment do
    first_filename = ENV['FIRST']
    second_filename = ENV['SECOND']
    if first_filename.nil? || second_filename.nil?
      puts "Requires FIRST and SECOND parameters to specify the 2 filenames!".red
      exit
    end
    unless File.file?(first_filename)
      puts "   FIRST file does not exist!".red
      exit
    end
    first_spreadsheet_file = Roo::Spreadsheet.open(first_filename)
    unless File.file?(second_filename)
      puts "   SECOND file does not exist!".red
      exit
    end
    File.delete('FIRST-diffs') if File.exist?('FIRST-diffs')
    File.delete('SECOND-diffs') if File.exist?('SECOND-diffs')
    second_spreadsheet_file = Roo::Spreadsheet.open(second_filename)
    compare_excel_spreadsheets(first_spreadsheet_file, second_spreadsheet_file)
  end
end
