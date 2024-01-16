class Thorax.Views.ImportQrdaPatients extends Thorax.Views.BonnieView
  template: JST['measure/import_qrda_patients']

  initialize: ->
    @measure = @model.get('cqmMeasure');


  context: ->
    _(super).extend
      token: $('meta[name="csrf-token"]').attr('content')

  setup: ->
    @importQrdaPatientsDialog = @$("#importQrdaPatientsDialog")

  events:
    rendered: ->
      @$el.on 'hidden.bs.modal', -> @remove() unless $('#importQrdaPatientsDialog').is(':visible')
    'click #importPatientsCancel': 'cancel'
    'click #importPatientsSubmit': 'submit'
    'change #patientFileInput': 'fileChanged'
    'ready': 'setup'
    

  display: ->
    @importQrdaPatientsDialog.modal(
      "backdrop" : "static",
      "keyboard" : true,
      "show" : true)

  cancel: ->
    @importQrdaPatientsDialog.modal('hide')

  submit: (e) ->
    e.preventDefault()
    $(e.target).prop('disabled', true)
    @$('form').submit()
    @importQrdaPatientsDialog.modal('hide')
    @$("#importQrdaPatientInProgressDialog").modal backdrop: 'static'

  fileChanged: (e) ->
    @$('#importPatientsSubmit').prop('disabled', !fileName = $(e.target).val())