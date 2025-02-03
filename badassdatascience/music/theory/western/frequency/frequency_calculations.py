import numpy as np

def calculate_frequency(
    midi_note_number, 
    method = '12-tone equal temperament',
    concert_frequency_a4 = 440.,
    concert_midi_note_number_a4 = 69,
):
    frequency = np.nan
    
    if method == '12-tone equal temperament':
        frequency = concert_frequency_a4 * (np.power(2., (1./12.)) ** (midi_note_number - concert_midi_note_number_a4))

    return frequency
