#
# load useful libraries
#
import numpy as np

#
# define a C major scale and its enumeration
#
# we use this to transpose while keeping pitch class names consistent
#
diatonic_c_major_scale_by_name = np.array(['C', 'D', 'E', 'F', 'G', 'A', 'B'], dtype='object')
diatonic_c_major_scale_enumerated = np.arange(0, len(diatonic_c_major_scale_by_name))

#
# define a C-based chromatic scale with its enharmonic spellings (up to two accidentals per spelling)
#
chromatic_c_based_enharmonic_spellings = [        
    ['Dbb', 'C', 'B#'],
    ['Db', 'C#', 'B##'],
    ['Ebb', 'D', 'C##'],
    ['Fbb', 'Eb', 'D#'],
    ['Fb', 'E', 'D##'],
    ['Gbb', 'F', 'E#'],
    ['Gb', 'F#', 'E##'],
    ['Abb', 'G', 'F##'],
    ['Ab', 'G#'],
    ['Bbb', 'A', 'G##'],
    ['Cbb', 'Bb', 'A#'],
    ['Cb', 'B', 'A##'],
]

chromatic_c_based_enharmonic_spellings_enumerated = np.arange(0, len(chromatic_c_based_enharmonic_spellings))

#
# some work on enharmonic spellings we might need later:
#
# there might be some cool math trick or broadcast I can use to populate these items; to avoid looping
#
note_to_enharmonic_by_name_dict = {}
note_to_enharmonic_by_accidental_dict = {}
for i in chromatic_c_based_enharmonic_spellings:
    for j in i:
        if not j in note_to_enharmonic_by_name_dict:
            note_to_enharmonic_by_name_dict[j] = {}
            note_to_enharmonic_by_accidental_dict[j] = {}
        for k in i:
            if k == j: continue
            note_to_enharmonic_by_name_dict[j][k[0]] = k[1:]
            note_to_enharmonic_by_accidental_dict[j][k[1:]] = k[0]

#
# test
#
diatonic_c_major_scale_by_name_constructed = np.array([], dtype='object')
for same_enharmonic_spellings in chromatic_c_based_enharmonic_spellings:
    for spelling in same_enharmonic_spellings:
        if spelling.find('b') == -1 and spelling.find('#') == -1:
            diatonic_c_major_scale_by_name_constructed = np.append(diatonic_c_major_scale_by_name_constructed, spelling)
            
np.max(np.int8(diatonic_c_major_scale_by_name_constructed == diatonic_c_major_scale_by_name)) == 1

#
# another formulation of the chromatic scale
#
chromatic_scale_basic = ['C', 'C#/Db', 'D', 'D#/Eb', 'E', 'F', 'F#/Gb', 'G', 'G#/Ab', 'A', 'A#/Bb', 'B']