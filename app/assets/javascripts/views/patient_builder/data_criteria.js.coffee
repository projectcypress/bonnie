# Abstract base class for children of the patient builder that need to communicate with the top-level via events
class Thorax.Views.BuilderChildView extends Thorax.Views.BonnieView
  events:
    ready: -> @patientBuilder().registerChild this

  patientBuilder: ->
    parent = @parent
    until parent instanceof Thorax.Views.PatientBuilder
      parent = parent.parent
    parent
  triggerMaterialize: ->
    @trigger 'bonnie:materialize'

  parseDataElementDescription: (description) ->
    # dataelements such as birthdate do not have descriptions
    if !description
      ""
    else
      description.split(/, (.*:.*)/)?[1] or description


class Thorax.Views.SelectCriteriaView extends Thorax.Views.BonnieView
  template: JST['patient_builder/select_criteria']
  events:
    rendered: ->
      # FIXME: We'd like to do this via straight thorax events, doesn't seem to work...
      @$('.collapse').on 'show.bs.collapse hide.bs.collapse', (e) =>
        $('a.panel-title[data-toggle="collapse"]').toggleClass('closed').find('.panel-icon').toggleClass('fa-3x fa-1x') #shrink others
        @$('.panel-expander').toggleClass('fa-angle-right fa-angle-down')
        @$('a.panel-title[data-toggle="collapse"]').toggleClass('closed')
        if e.type is 'show' then $('a.panel-title[data-toggle="collapse"]').next('div.in').not(e.target).collapse('hide') # hide open ones

  icon: -> @collection.first()?.icon()


class Thorax.Views.SelectCriteriaItemView extends Thorax.Views.BuilderChildView
  addCriteriaToPatient: -> @trigger 'bonnie:dropCriteria', @model
  context: ->
    desc = @parseDataElementDescription @model.get('description')
    _(super).extend
      type: desc.split(": ")[0]
      # everything after the data criteria type is the detailed description
      detail: desc.substring(desc.indexOf(':')+2)

class Thorax.Views.EditCriteriaView extends Thorax.Views.BuilderChildView
  className: 'patient-criteria'

  @highlight:
    partial: 'highlight-partial'
    valid: 'highlight-valid'

  template: JST['patient_builder/edit_criteria']

  options:
    serialize: { children: false }
    populate: { context: true, children: false }

  initialize: ->
    if DataCriteriaHelpers.isPrimaryCodePathSupported @model.get('dataElement')
      codes = DataCriteriaHelpers.getPrimaryCodes @model.get('dataElement')
      code_list_id = @model.get('codeListId')
      vs = (@measure.get('cqmValueSets').find( (vs) => vs.id is code_list_id) )
      concepts = vs?.compose?.include || []

      @editCodesDisplayView = new Thorax.Views.EditCodesDisplayView codes: codes, measure: @measure, parent: @
      @editCodeSelectionView = new Thorax.Views.EditCodeSelectionView codes: codes, concepts: concepts, measure: @measure, parent: @

      if codes.length is 0
        @editCodeSelectionView.addDefaultCodeToDataElement()
    else
      @unsupportedCodesView = new Thorax.Views.UnsupportedCodesView primaryCodePath: DataCriteriaHelpers.getPrimaryCodePath @model.get('dataElement')

    @timingAttributeViews = []
    for timingAttr in @model.getPrimaryTimingAttributes()
      initialValue = @model.get('dataElement').fhir_resource[timingAttr.name]
      switch timingAttr.type
        when 'Period'
          dateInterval = DataTypeHelpers.createIntervalFromPeriod(initialValue)
          intervalView = new Thorax.Views.InputIntervalDateTimeView(
            initialValue: dateInterval,
            attributeName: timingAttr.name, attributeTitle: timingAttr.title,
            showLabel: true, defaultYear: @measure.getMeasurePeriodYear())
          @timingAttributeViews.push intervalView
          @listenTo intervalView, 'valueChanged', @updateDateInputChange
        when 'dateTime', 'instant'
          dateTime = DataTypeHelpers.getCQLDateTimeFromString(initialValue?.value)
          dateTimeView = new Thorax.Views.InputCqlDateTimeView(
            initialValue: dateTime,
            attributeName: timingAttr.name, attributeTitle: timingAttr.title,
            showLabel: true, defaultYear: @measure.getMeasurePeriodYear())
          @timingAttributeViews.push dateTimeView
          @listenTo dateTimeView, 'valueChanged', @updateDateInputChange
        when 'date'
          date = DataTypeHelpers.getCQLDateTimeFromString(initialValue?.value)
          dateView = new Thorax.Views.InputDateView(
            initialValue: date,
            attributeName: timingAttr.name, attributeTitle: timingAttr.title,
            showLabel: true, defaultYear: @measure.getMeasurePeriodYear())
          @timingAttributeViews.push dateView
          @listenTo dateView, 'valueChanged', @updateDateInputChange

    # view that shows all the currently set attributes
    @attributeDisplayView = new Thorax.Views.DataCriteriaAttributeDisplayView(model: @model)
    @listenTo @attributeDisplayView, 'attributesModified', @attributesModified

    # view that allows for editing new attribute values
    @attributeEditorView = new Thorax.Views.DataCriteriaAttributeEditorView(model: @model)
    @listenTo @attributeEditorView, 'attributesModified', @attributesModified

    # view that shows all the extensions of the resource
    @displayExtensionsView = new Thorax.Views.DisplayExtensionsView(dataElement: @model.get('dataElement'), extensionsAccessor: 'extension')
    @listenTo @displayExtensionsView, 'extensionModified', @extensionModified

    # view that adds extensions to the resource
    @addExtensionsView = new Thorax.Views.AddExtensionsView(dataElement: @model.get('dataElement'), extensionsAccessor: 'extension')
    @listenTo @addExtensionsView, 'extensionModified', @extensionModified

    # view that shows all the modifier extensions of the resource
    @displayModifierExtensionsView = new Thorax.Views.DisplayExtensionsView(dataElement: @model.get('dataElement'), extensionsAccessor: 'modifierExtension')
    @listenTo @displayModifierExtensionsView, 'extensionModified', @modifierExtensionModified

    # view that adds modifier extensions to the resource
    @addModifierExtensionsView = new Thorax.Views.AddExtensionsView(dataElement: @model.get('dataElement'), extensionsAccessor: 'modifierExtension')
    @listenTo @addModifierExtensionsView, 'extensionModified', @modifierExtensionModified

    # view that allows for negating the data criteria, will not display on non-negateable data criteria
#    @negationRationaleView = new Thorax.Views.InputCodingView({ cqmValueSets: @measure.get('cqmValueSets'), codeSystemMap: @measure.codeSystemMap(), attributeName: 'negationRationale', initialValue: @model.get('dataElement').negationRationale })
#    @listenTo @negationRationaleView, 'valueChanged', @updateAttributeFromInputChange

    @model.on 'highlight', (type) =>
      @$('.criteria-data').addClass(type)
      @$('.highlight-indicator').attr('tabindex', 0).text 'matches selected logic, '

  updateCodes: (codes) ->
    DataCriteriaHelpers.setPrimaryCodes @model.get('dataElement'), codes
    if codes.length is 0
      @editCodeSelectionView.addDefaultCodeToDataElement()
    else
      @editCodeSelectionView.codes = codes
      @editCodesDisplayView.codes = codes
      @editCodesDisplayView.render()
      @triggerMaterialize()
    null

  context: ->
    desc = @parseDataElementDescription(@model.get('description'))
    resourceType = @model.get('fhir_resource').resourceType
    category = DataCriteriaHelpers.DATA_ELEMENT_CATEGORIES[resourceType] || 'unsupported'
    definition_title = category.replace(/_/g, ' ').replace(/(^|\s)([a-z])/g, (m,p1,p2) -> return p1+p2.toUpperCase())
    if desc.split(": ")[0] is definition_title
      desc = desc.substring(desc.indexOf(':') + 2)
    primaryTimingAttribute = @model.getPrimaryTimingAttribute()
    primaryTimingValue = @model.get('dataElement').fhir_resource[primaryTimingAttribute?.name]
    if primaryTimingAttribute?.type == 'Period'
      primaryTimingValue = DataTypeHelpers.createIntervalFromPeriod primaryTimingValue
    else if primaryTimingAttribute?.type == 'dateTime' || primaryTimingAttribute?.type == 'instant'
      primaryTimingValue = DataTypeHelpers.getCQLDateTimeFromString primaryTimingValue?.value
    else if primaryTimingAttribute?.type == 'date'
      primaryTimingValue = DataTypeHelpers.getCQLDateFromString primaryTimingValue?.value

    _(super).extend
      # When we create the form and populate it, we want to convert times to moment-formatted dates
      start_date: moment.utc(primaryTimingValue.low.toJSDate()).format('L') if primaryTimingValue?.low?
      start_time: moment.utc(primaryTimingValue.low.toJSDate()).format('LT') if primaryTimingValue?.low?
      start_date: moment.utc(primaryTimingValue.toJSDate()).format('L') if primaryTimingValue?.isDateTime
      start_time: moment.utc(primaryTimingValue.toJSDate()).format('LT') if primaryTimingValue?.isDateTime
      start_date: moment.utc(primaryTimingValue.toJSDate()).format('L') if primaryTimingValue?.isDate
      end_date: moment.utc(primaryTimingValue.high.toJSDate()).format('L') if primaryTimingValue?.high?
      end_time: moment.utc(primaryTimingValue.high.toJSDate()).format('LT') if primaryTimingValue?.high?
      end_date_is_undefined: !primaryTimingValue?.high?
      description: desc
      value_sets: @model.measure()?.valueSets() or []
      icon: @model.icon()
      definition_title: definition_title
      fhirId: @model.get('dataElement').fhir_resource.id
      canHaveNegation: @model.canHaveNegation()
      isPeriod: @model.isPeriodType() && !@model.get('negation') # if something is negated, it didn't happen so is not a period

  # When we serialize the form, we want to convert formatted dates back to times
  events:
    serialize: (attr) ->
    rendered: ->
      @$('.criteria-data.droppable').droppable greedy: true, accept: '.ui-draggable', hoverClass: 'drop-target-highlight', drop: _.bind(@dropCriteria, this)
      @$el.toggleClass 'during-measurement-period', @isDuringMeasurePeriod()
    # hide date-picker if it's still visible and focus is not on a .date-picker input (occurs with JAWS SR arrow-key navigation)
    'focus .form-control': (e) -> if not @$(e.target).hasClass('date-picker') and $('.datepicker').is(':visible') then @$('.date-picker').datepicker('hide')
#    'change .negation-select': 'toggleNegationSelect'

  updateAttributeFromInputChange: (inputView) ->
    @model.get('dataElement').fhir_resource[inputView.attributeName] = inputView.value
    @triggerMaterialize()

  updateDateInputChange: (inputView) ->
    attributes = DataCriteriaHelpers.PRIMARY_TIMING_ATTRIBUTES[@model.get('dataElement').fhir_resource.resourceType]
    if attributes[inputView.attributeName] == 'Period'
      @model.get('dataElement')
        .fhir_resource[inputView.attributeName] = DataTypeHelpers.createPeriodFromInterval(inputView.value)
    else if attributes[inputView.attributeName] == 'instant'
      @model.get('dataElement')
        .fhir_resource[inputView.attributeName] = DataTypeHelpers.getPrimitiveInstantForCqlDateTime inputView.value
    else if attributes[inputView.attributeName] == 'dateTime'
      @model.get('dataElement')
        .fhir_resource[inputView.attributeName] = DataTypeHelpers.getPrimitiveDateTimeForCqlDateTime inputView.value
    else if attributes[inputView.attributeName] == 'date'
      @model.get('dataElement')
        .fhir_resource[inputView.attributeName] = DataTypeHelpers.getPrimitiveDateForCqlDate inputView.value
    @triggerMaterialize()

  attributesModified: ->
    @attributeDisplayView.render()
    @triggerMaterialize()

  extensionModified: ->
    @displayExtensionsView.render()
    @triggerMaterialize()

  modifierExtensionModified: ->
    @displayModifierExtensionsView.render()
    @triggerMaterialize()

  isDuringMeasurePeriod: ->
    timingAttribute = @model.get('dataElement').fhir_resource[@model.getPrimaryTimingAttribute()?.name]
    if !timingAttribute?
      return false
    if timingAttribute.start || timingAttribute.end
      interval = DataTypeHelpers.createIntervalFromPeriod(timingAttribute)
      interval.low?.year is interval.high?.year is @model.measure().getMeasurePeriodYear()
    else if (timingAttribute.value)
      cqlDate = DataTypeHelpers.getCQLDateFromString(timingAttribute.value)
      cqlDate?.year is @model.measure().getMeasurePeriodYear()

  # Copy timing attributes (relevantPeriod, prevelancePeriod etc..) onto the criteria being dragged from the criteria it is being dragged ontop of
  copyTimingAttributes: (droppedCriteria, targetCriteria) ->
    droppedCriteriaTiming = droppedCriteria.getPrimaryTimingAttributes()
    targetCriteriaTiming = targetCriteria.getPrimaryTimingAttributes()

    # if the target has no timing attributes, we have nothing to copy
    if targetCriteriaTiming.length == 0
      return

    # do a pairwise copy from target timing attributes to dropped timing attributes
    for timingIndex, droppedTimingAttr of droppedCriteriaTiming
      # if there is a corresponding pair, then copy it
      if timingIndex < targetCriteriaTiming.length
        @_copyTimingAttribute(droppedCriteria, targetCriteria, droppedTimingAttr, targetCriteriaTiming[timingIndex])
      # otherwise, copy the first one in target
      else
        @_copyTimingAttribute(droppedCriteria, targetCriteria, droppedTimingAttr, targetCriteriaTiming[0])

  # Helper function to copy a timing value from targetCriteria to droppedCriteria.
  _copyTimingAttribute: (droppedCriteria, targetCriteria, droppedAttr, targetAttr) ->
    targetResource = targetCriteria.get('dataElement').fhir_resource
    droppedResource = droppedCriteria.get('dataElement').fhir_resource
    # clone if they are of the same type
    if targetAttr.type == droppedAttr.type
      if targetResource[targetAttr.name]?
        droppedResource[droppedAttr.name] = targetResource[targetAttr.name].clone()
      else
        droppedResource[droppedAttr.name] = null

    # turn Period into DateTime
    else if targetAttr.type == 'Period' && droppedAttr.type == 'dateTime'
      if targetResource[targetAttr.name]?.start?
        droppedResource[droppedAttr.name] =
          DataTypeHelpers.getPrimitiveDateTimeForStringDateTime(targetResource[targetAttr.name].start?.value)
      else
        droppedResource[droppedAttr.name] = null

    # turn DateTime into Period. use start + 15 minutes for end.
    else if targetAttr.type == 'dateTime' && droppedAttr.type == 'Period'
      if targetResource[targetAttr.name]?
        droppedResource[droppedAttr.name] =
          DataTypeHelpers.getPeriodForStringDateTime(targetResource[targetAttr.name].value)
      else
        droppedResource[droppedAttr.name] = null

    # turn DateTime into date.
    else if targetAttr.type == 'dateTime' && droppedAttr.type == 'date'
      if targetResource[targetAttr.name]?
        droppedResource[droppedAttr.name] =
          DataTypeHelpers.getPrimitiveDateForStringDateTime(targetResource[targetAttr.name].value)
      else
        droppedResource[droppedAttr.name] = null

    # turn date into dateTime.
    else if targetAttr.type == 'date' && droppedAttr.type == 'dateTime'
      if targetResource[targetAttr.name]?
        droppedResource[droppedAttr.name] =
          DataTypeHelpers.getPrimitiveDateTimeForStringDate(targetResource[targetAttr.name].value)
      else
        droppedResource[droppedAttr.name] = null

    # turn period into date.
    else if targetAttr.type == 'Period' && droppedAttr.type == 'date'
      if targetResource[targetAttr.name]?
        droppedResource[droppedAttr.name] =
          DataTypeHelpers.getPrimitiveDateForStringDateTime(targetResource[targetAttr.name].start?.value)
      else
        droppedResource[droppedAttr.name] = null

    # turn date into period.
    else if targetAttr.type == 'date' && droppedAttr.type == 'Period'
      if targetResource[targetAttr.name]?
        droppedResource[droppedAttr.name] =
          DataTypeHelpers.getPeriodForStringDate(targetResource[targetAttr.name].value)
      else
        droppedResource[droppedAttr.name] = null

  dropCriteria: (e, ui) ->
    # When we drop a new criteria on an existing criteria
    droppedCriteria = $(ui.draggable).model().clone()
    droppedCriteria.setNewId()

    targetCriteria = $(e.target).model()
    @copyTimingAttributes(droppedCriteria, targetCriteria)
    @trigger 'bonnie:dropCriteria', droppedCriteria
    return false

  isExpanded: -> @$('form').is ':visible'

#  toggleNegationSelect: (e) ->
#    if $(e.target).is(":checked")
#      @$('.negationRationaleCodeEntry').removeClass('hidden')
#    else
#      @$('.negationRationaleCodeEntry').addClass('hidden')
#      @model.get('dataElement').negationRationale = null
#      @model.set('negation', false, {silent: true})
#    @negationRationaleView.resetCodeSelection()

  toggleDetails: (e) ->
    e.preventDefault()
    @$('.criteria-details, form').toggleClass('hide')
    @$('.criteria-type-marker').toggleClass('open')
    unless @isExpanded()
      @serialize(children: false)
      # FIXME sortable: commenting out due to odd bug in droppable
      # @model.trigger 'close', @model
      @render() # re-sorting the collection will re-render this view, remove this if we use above approach
    @$(':focusable:visible:first').focus()

  showDelete: (e) ->
    e.preventDefault()
    $btn = $(e.currentTarget)
    $btn.toggleClass('btn-danger btn-danger-inverse').next().toggleClass('hide')

  removeCriteria: (e) ->
    e.preventDefault()
    # Remove attributes from other resources/criteria that reference this resource
    @parent.parent.removeReferenceAttributes(@model.get('dataElement').fhir_resource.id)
    @model.destroy()
