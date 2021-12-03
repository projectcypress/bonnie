module CQM
  # Measure contains the information necessary to represent the CQL version of a Clinical Quality Measure,
  # As needed by the Bonnie & Cypress applications
  class Measure
    belongs_to :group, optional: true
    scope :by_user, ->(user) { where group_id: user.current_group.id }
    index 'group_id' => 1
    index 'group_id' => 1, 'hqmf_set_id' => 1
    has_and_belongs_to_many :patients, class_name: 'CQM::Patient'
    # Find the measures matching a patient
    def self.for_patient(record)
      where group_id: record.group_id, hqmf_set_id: { '$in' => record.measure_ids }
    end

    def save_self_and_child_docs
      save!
      package.save! if package.present?
      value_sets.each(&:save!)
    end

    def associate_self_and_child_docs_to_group(group)
      self.group = group
      package.group = group if package.present?
      value_sets.each { |vs| vs.group = group }
    end

    # note that this method doesn't change the _id of embedded documents, but that should be fine
    def make_fresh_duplicate
      m2 = dup_and_remove_user(self)
      m2.package = dup_and_remove_user(package) if package.present?
      m2.value_sets = value_sets.map { |vs| dup_and_remove_user(vs) }
      return m2
    end

    def delete_self_and_child_docs
      package.delete if package.present?
      value_sets.each(&:delete)
      delete
    end

    def destroy_self_and_child_docs
      package.destroy if package.present?
      value_sets.each(&:destroy)
      destroy
    end

    # A measure may have 1 or more population sets that may have 1 or more stratifications
    # This method returns an array of hashes with the population_set and stratification_id for every combindation
    def population_sets_and_stratifications_for_measure
      population_set_array = []
      population_sets.each do |population_set|
        population_set_hash = { population_set_id: population_set.population_set_id }
        next if population_set_array.include? population_set_hash

        population_set_array << population_set_hash
      end
      population_sets.each do |population_set|
        population_set.stratifications.each do |stratification|
          population_set_stratification_hash = { population_set_id: population_set.population_set_id,
                                                 stratification_id: stratification.stratification_id }
          population_set_array << population_set_stratification_hash
        end
      end
      population_set_array
    end

    # This method returns the population_set for a given 'population_set_key.'  The popluation_set_key is the key used
    # by the cqm-execution-service to reference the population set for a specific set of calculation results
    def population_set_for_key(population_set_key)
      ps_hash = population_sets_and_stratifications_for_measure
      ps_hash.keep_if { |ps| [ps[:population_set_id], ps[:stratification_id]].include? population_set_key }
      return nil if ps_hash.blank?

      [population_sets.where(population_set_id: ps_hash[0][:population_set_id]).first, ps_hash[0][:stratification_id]]
    end

    # This method returns an population_set_hash (from the population_sets_and_stratifications_for_measure)
    # for a given 'population_set_key.' The popluation_set_key is the key used by the cqm-execution-service
    # to reference the population set for a specific set of calculation results
    def population_set_hash_for_key(population_set_key)
      population_set_hash = population_sets_and_stratifications_for_measure
      population_set_hash.keep_if { |ps| [ps[:population_set_id], ps[:stratification_id]].include? population_set_key }.first
    end

    # This method returns a popluation_set_key for.a given population_set_hash
    def key_for_population_set(population_set_hash)
      population_set_hash[:stratification_id] || population_set_hash[:population_set_id]
    end

    # This method returns the subset of population keys used in a specific measure
    def population_keys
      %w[IPP DENOM NUMER NUMEX DENEX DENEXCEP MSRPOPL MSRPOPLEX].keep_if { |pop| population_sets.first.populations[pop] && population_sets.first.populations[pop]['hqmf_id'] }
    end

    private

    def dup_and_remove_user(mongoid_doc)
      new_doc = mongoid_doc.dup
      new_doc.group = nil
      return new_doc
    end

  end

  class PopulationSet
    def bonnie_result_criteria_names
      criteria = populations.as_json.keys
      criteria << 'OBSERV' if observations.present?
      return CQM::Measure::ALL_POPULATION_CODES & criteria # do this last to ensure ordering
    end
  end

  class Stratification
    def bonnie_result_criteria_names
      criteria = population_set.bonnie_result_criteria_names + ['STRAT']
      return CQM::Measure::ALL_POPULATION_CODES & criteria # do this last to ensure ordering
    end
  end
end
