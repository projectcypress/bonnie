describe 'Patient', ->

  beforeAll ->
    jasmine.getJSONFixtures().clearCache()
    patients = []
    # patients.push(getJSONFixture('patients/CMS160v6/Expired_DENEX.json'))
    patients.push(getJSONFixture('fhir_patients/CMS1010V0/john_smith.json'))
    collection = new Thorax.Collections.Patients patients, parse: true
    @patient = collection.first()
    # @patient1 = collection.at(1)

  it 'has basic attributes available', ->
    expect(@patient.get('cqmPatient').fhir_patient.gender.value).toEqual 'unknown'

  it 'correctly performs deep cloning', ->
    clone = @patient.deepClone()
    expect(clone.cid).not.toEqual @patient.cid
    expect(clone.keys()).toEqual @patient.keys()
    # I am not convinced that the sdc collection needs a CID, it's not getting set on initialization
    # expect(clone.get('source_data_criteria').cid).not.toEqual @patient.get('source_data_criteria').cid
    expect(clone.get('source_data_criteria').pluck('id')).toEqual @patient.get('source_data_criteria').pluck('id')
    cloneNewId = @patient.deepClone(new_id: true)
    expect(cloneNewId.cid).not.toEqual @patient.cid
    expect(cloneNewId.get('cqmPatient').id.toString()).not.toEqual @patient.get('cqmPatient').id.toString()

  it 'correctly deduplicates the name when deep cloning and dedupName is an option', ->
    clone = @patient.deepClone({dedupName: true})
    expect(clone.getFirstName()).toEqual @patient.getFirstName() + " (1)"

  it 'updates patient race', ->
    race = {code: '2106-3', display: 'White'}
    @patient.setCqmPatientRace(race)
    raceExt = @patient.get('cqmPatient').fhir_patient.extension.find (ext) ->
      ext.url.value == 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-race'
    expect(raceExt.extension[0].value.code.value).toEqual race.code
    expect(raceExt.extension[0].value.display.value).toEqual race.display

  it 'updates patient ethnicity', ->
    ethnicity = {code: '2186-5', display: 'Not Hispanic or Latino'}
    @patient.setCqmPatientEthnicity(ethnicity)
    ethnicityExt = @patient.get('cqmPatient').fhir_patient.extension.find (ext) ->
      ext.url.value == 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity'
    expect(ethnicityExt.extension[0].value.code.value).toEqual ethnicity.code
    expect(ethnicityExt.extension[0].value.display.value).toEqual ethnicity.display

  # it 'correctly sorts criteria by multiple attributes', ->
  #   # Patient has for existing criteria; first get their current order
  #   startOrder = @patient1.get('source_data_criteria').map (dc) -> dc.cid
  #   # Set some attribute values so that they should sort 4,3,2,1 and sort
  #   @patient1.get('source_data_criteria').at(0).set start_date: 2, end_date: 2
  #   @patient1.get('source_data_criteria').at(1).set start_date: 2, end_date: 1
  #   @patient1.get('source_data_criteria').at(2).set start_date: 1, end_date: 3
  #   @patient1.get('source_data_criteria').at(3).set start_date: 1, end_date: 2
  #   @patient1.sortCriteriaBy 'start_date', 'end_date'
  #   expect(@patient1.get('source_data_criteria').at(0).cid).toEqual startOrder[3]
  #   expect(@patient1.get('source_data_criteria').at(1).cid).toEqual startOrder[2]
  #   expect(@patient1.get('source_data_criteria').at(2).cid).toEqual startOrder[1]
  #   expect(@patient1.get('source_data_criteria').at(3).cid).toEqual startOrder[0]

  describe 'validation', ->

    it 'passes patient with no issues', ->
      errors = @patient.validate()
      expect(errors).toBeUndefined()

    it 'fails patient missing a first name', ->
      clone = @patient.deepClone()
      clone.get('cqmPatient').fhir_patient.name[0].given[0].value = ''
      errors = clone.validate()
      expect(errors.length).toEqual 1
      expect(errors[0][2]).toEqual 'Name fields cannot be blank'

    it 'fails patient missing a last name', ->
      clone = @patient.deepClone()
      clone.get('cqmPatient').fhir_patient.name[0].family.value = ''
      errors = clone.validate()
      expect(errors.length).toEqual 1
      expect(errors[0][2]).toEqual 'Name fields cannot be blank'

    it 'fails patient missing a birthdate', ->
      clone = @patient.deepClone()
      clone.get('cqmPatient').fhir_patient.birthDate.value = undefined
      errors = clone.validate()
      expect(errors.length).toEqual 1
      expect(errors[0][2]).toEqual 'Date of birth cannot be blank'

    # it 'fails deceased patient without a deathdate', ->
    #   clone = @patient.deepClone()
    #   (clone.get('cqmPatient').qdmPatient.patient_characteristics().filter (elem) -> elem.qdmStatus == 'expired')[0].expiredDatetime = undefined
    #   errors = clone.validate()
    #   expect(errors.length).toEqual 1
    #   expect(errors[0][2]).toEqual 'Deceased patient must have date of death'
