
# Input view for Integer types.
class Thorax.Views.InputPositiveIntegerView extends Thorax.Views.BonnieView
  template: JST['patient_builder/inputs/positive_integer']

  # Expected options to be passed in using the constructor options hash:
  #   initialValue - positive integer - Optional. Initial value of positive integer.
  #   placeholder - string - Optional. placeholder text to show. will use 'positive integer' if not specified
  initialize: ->
    if @initialValue?
      @value = @initialValue
    else
      @value = null
    @invalidInput = false

  events:
    'change input': 'handleInputChange'
    'keyup input': 'handleInputChange'

  # checks if the value in this view is valid. returns true or false. this is used by the attribute entry view to determine
  # if the add button should be active or not
  hasValidValue: ->
    @value?

  # checks if an invalid value is entered
  # hasInvalidInput is true when there is an entered value which is invalid or cannot be parsed
  hasInvalidInput: ->
    @invalidInput

  handleInputChange: (e) ->
    inputValue = @$(e.target).val()
    if /^[1-9][0-9]*$/.test(inputValue)
      parsed = parseInt(inputValue)
      if isNaN(parsed)
        @value = null
      else
        @value = cqm.models.PrimitivePositiveInt.parsePrimitive(parsed)
    else
      @value = null

    if @value? || !inputValue
      @$(e.target).parent().removeClass('has-error')
      @invalidInput = false
    else
      @$(e.target).parent().addClass('has-error')
      @invalidInput = true

    @trigger 'valueChanged', @
