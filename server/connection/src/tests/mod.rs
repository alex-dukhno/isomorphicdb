// Copyright 2020 - present Alex Dukhno
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[cfg(test)]
mod accept_client_request;
mod async_io;
#[cfg(test)]
mod connection;
#[cfg(test)]
mod pg_frontend;

// tests/fixtures/identity.pfx dumped and stored here for tests
fn certificate_content() -> Vec<u8> {
    vec![
        48, 130, 9, 81, 2, 1, 3, 48, 130, 9, 23, 6, 9, 42, 134, 72, 134, 247, 13, 1, 7, 1, 160, 130, 9, 8, 4, 130, 9,
        4, 48, 130, 9, 0, 48, 130, 3, 183, 6, 9, 42, 134, 72, 134, 247, 13, 1, 7, 6, 160, 130, 3, 168, 48, 130, 3, 164,
        2, 1, 0, 48, 130, 3, 157, 6, 9, 42, 134, 72, 134, 247, 13, 1, 7, 1, 48, 28, 6, 10, 42, 134, 72, 134, 247, 13,
        1, 12, 1, 6, 48, 14, 4, 8, 43, 254, 232, 49, 254, 17, 242, 156, 2, 2, 8, 0, 128, 130, 3, 112, 128, 85, 237, 49,
        112, 9, 42, 198, 225, 223, 122, 113, 39, 229, 63, 238, 174, 25, 255, 199, 94, 71, 123, 224, 72, 103, 219, 0,
        46, 22, 54, 241, 24, 36, 125, 236, 122, 6, 181, 199, 4, 190, 155, 215, 107, 168, 112, 217, 147, 146, 194, 189,
        153, 207, 189, 15, 20, 67, 220, 175, 222, 107, 96, 226, 4, 31, 207, 178, 215, 146, 40, 246, 171, 21, 168, 109,
        82, 154, 228, 35, 56, 66, 155, 254, 107, 24, 209, 194, 215, 33, 211, 191, 220, 81, 249, 31, 133, 164, 90, 217,
        116, 14, 220, 14, 178, 167, 161, 140, 101, 17, 89, 35, 218, 151, 19, 179, 37, 195, 83, 25, 250, 49, 236, 146,
        113, 224, 106, 216, 247, 135, 82, 4, 137, 217, 43, 187, 6, 199, 58, 132, 239, 106, 85, 212, 64, 142, 239, 222,
        111, 37, 116, 75, 25, 210, 161, 149, 112, 249, 4, 84, 209, 237, 119, 246, 27, 121, 234, 204, 30, 223, 5, 131,
        64, 145, 224, 0, 4, 60, 55, 132, 57, 210, 199, 212, 196, 73, 108, 91, 5, 37, 127, 63, 242, 76, 191, 168, 237,
        109, 28, 38, 194, 234, 30, 117, 43, 134, 207, 111, 169, 145, 33, 68, 215, 123, 172, 76, 208, 228, 93, 70, 38,
        124, 221, 15, 74, 37, 107, 240, 254, 165, 60, 117, 114, 195, 165, 182, 3, 4, 92, 118, 38, 106, 55, 245, 1, 131,
        27, 164, 188, 4, 60, 178, 190, 75, 86, 119, 211, 157, 145, 82, 12, 211, 196, 99, 149, 165, 109, 152, 45, 46,
        53, 194, 169, 229, 254, 22, 166, 159, 122, 193, 177, 101, 34, 25, 46, 53, 22, 82, 33, 185, 10, 44, 24, 153,
        240, 45, 60, 105, 181, 209, 243, 100, 214, 45, 55, 58, 212, 23, 99, 171, 150, 174, 90, 216, 140, 37, 126, 240,
        232, 58, 9, 216, 190, 24, 93, 173, 48, 104, 108, 48, 229, 151, 232, 154, 164, 42, 191, 122, 35, 111, 59, 75,
        250, 74, 13, 40, 134, 140, 8, 148, 120, 105, 100, 116, 68, 226, 125, 143, 142, 127, 152, 228, 7, 18, 41, 47,
        22, 97, 63, 152, 106, 162, 152, 12, 158, 141, 82, 44, 54, 108, 101, 233, 29, 127, 164, 151, 63, 132, 126, 136,
        243, 12, 129, 92, 246, 159, 99, 0, 29, 195, 223, 76, 9, 248, 244, 71, 69, 152, 160, 150, 16, 86, 203, 27, 141,
        217, 106, 45, 87, 144, 96, 60, 202, 8, 118, 88, 93, 98, 169, 45, 11, 102, 179, 33, 235, 181, 249, 146, 97, 214,
        92, 42, 174, 126, 165, 251, 67, 145, 215, 104, 227, 203, 62, 155, 198, 211, 198, 116, 120, 238, 78, 108, 165,
        36, 253, 239, 156, 172, 80, 80, 226, 225, 145, 46, 54, 168, 4, 210, 30, 11, 54, 81, 101, 252, 214, 106, 133,
        138, 237, 151, 135, 249, 150, 102, 227, 130, 163, 245, 169, 62, 163, 12, 206, 11, 130, 85, 57, 165, 157, 21,
        136, 84, 53, 228, 164, 125, 52, 7, 236, 253, 47, 111, 35, 12, 223, 141, 174, 203, 94, 113, 8, 133, 36, 165,
        172, 96, 214, 253, 41, 132, 12, 107, 246, 248, 11, 171, 201, 20, 66, 135, 199, 51, 39, 239, 92, 120, 36, 85,
        210, 201, 33, 97, 71, 223, 13, 136, 5, 21, 24, 243, 23, 69, 135, 254, 110, 189, 131, 183, 21, 190, 27, 0, 124,
        244, 114, 168, 78, 215, 141, 161, 83, 218, 11, 113, 71, 202, 97, 13, 149, 81, 228, 236, 241, 64, 245, 241, 57,
        168, 168, 69, 132, 231, 77, 39, 89, 229, 51, 247, 240, 167, 188, 31, 14, 209, 232, 25, 117, 65, 141, 53, 15,
        23, 67, 239, 71, 174, 120, 145, 185, 216, 201, 155, 214, 51, 244, 233, 14, 118, 101, 12, 44, 60, 165, 55, 26,
        138, 52, 229, 120, 169, 208, 63, 236, 70, 115, 36, 180, 16, 224, 3, 162, 70, 199, 244, 141, 93, 240, 180, 189,
        64, 33, 140, 88, 64, 17, 51, 130, 143, 147, 38, 66, 170, 228, 126, 165, 116, 50, 126, 195, 216, 215, 60, 174,
        242, 10, 213, 160, 164, 177, 63, 221, 198, 56, 48, 217, 159, 238, 249, 18, 118, 31, 138, 191, 188, 4, 154, 137,
        22, 218, 213, 33, 216, 3, 244, 130, 157, 53, 46, 79, 198, 11, 110, 146, 209, 55, 212, 172, 217, 10, 31, 165,
        220, 117, 221, 166, 233, 173, 27, 248, 92, 224, 11, 25, 151, 62, 242, 214, 201, 136, 203, 124, 61, 122, 127,
        86, 104, 225, 78, 26, 2, 211, 211, 4, 145, 16, 221, 128, 89, 82, 216, 157, 254, 82, 128, 108, 106, 134, 37,
        229, 118, 227, 93, 78, 61, 180, 159, 226, 18, 88, 157, 178, 241, 107, 31, 134, 119, 2, 62, 206, 150, 97, 84,
        227, 28, 5, 255, 28, 21, 190, 56, 232, 145, 137, 237, 18, 196, 250, 69, 61, 248, 116, 136, 209, 16, 77, 95, 18,
        26, 244, 119, 224, 116, 253, 219, 71, 62, 142, 144, 58, 108, 69, 101, 109, 43, 37, 219, 234, 25, 176, 208, 162,
        48, 221, 227, 6, 158, 125, 63, 208, 69, 48, 130, 5, 65, 6, 9, 42, 134, 72, 134, 247, 13, 1, 7, 1, 160, 130, 5,
        50, 4, 130, 5, 46, 48, 130, 5, 42, 48, 130, 5, 38, 6, 11, 42, 134, 72, 134, 247, 13, 1, 12, 10, 1, 2, 160, 130,
        4, 238, 48, 130, 4, 234, 48, 28, 6, 10, 42, 134, 72, 134, 247, 13, 1, 12, 1, 3, 48, 14, 4, 8, 134, 100, 141,
        166, 68, 73, 2, 56, 2, 2, 8, 0, 4, 130, 4, 200, 71, 21, 228, 62, 26, 50, 194, 187, 52, 75, 124, 143, 71, 128,
        96, 148, 94, 252, 206, 189, 6, 142, 157, 141, 152, 68, 175, 41, 71, 156, 122, 188, 175, 5, 207, 40, 12, 8, 66,
        110, 49, 103, 92, 212, 80, 99, 24, 176, 254, 179, 235, 54, 102, 130, 99, 100, 87, 65, 51, 185, 166, 95, 9, 163,
        124, 206, 186, 140, 78, 155, 68, 150, 245, 24, 212, 99, 76, 30, 93, 200, 107, 165, 157, 202, 207, 46, 231, 185,
        194, 64, 213, 215, 254, 98, 20, 68, 226, 11, 178, 151, 68, 240, 147, 81, 27, 66, 197, 84, 167, 73, 197, 0, 210,
        4, 217, 104, 43, 196, 7, 176, 29, 244, 254, 110, 86, 56, 197, 184, 192, 60, 99, 65, 245, 125, 136, 69, 208, 26,
        92, 133, 236, 195, 29, 187, 46, 216, 231, 101, 188, 182, 221, 49, 32, 141, 255, 83, 240, 240, 75, 109, 234, 91,
        61, 208, 50, 166, 202, 4, 162, 72, 218, 207, 227, 32, 186, 156, 142, 99, 17, 75, 199, 177, 56, 25, 16, 251,
        179, 63, 235, 35, 88, 157, 159, 240, 150, 21, 228, 139, 114, 50, 151, 30, 78, 63, 184, 97, 207, 40, 212, 190,
        70, 102, 9, 18, 34, 32, 203, 146, 153, 51, 61, 205, 254, 192, 143, 227, 6, 28, 236, 1, 89, 138, 126, 2, 44,
        216, 50, 85, 247, 142, 126, 63, 38, 120, 193, 115, 117, 238, 196, 198, 1, 219, 236, 101, 237, 156, 33, 140,
        199, 25, 112, 219, 240, 40, 79, 163, 170, 183, 45, 129, 34, 207, 52, 123, 208, 100, 58, 249, 179, 206, 145, 73,
        54, 145, 158, 192, 178, 191, 72, 137, 244, 47, 76, 235, 198, 180, 173, 68, 150, 241, 173, 203, 202, 83, 131,
        162, 84, 93, 137, 228, 210, 254, 13, 64, 19, 189, 170, 54, 240, 181, 223, 0, 22, 247, 110, 23, 20, 165, 196,
        224, 186, 237, 236, 196, 114, 147, 221, 17, 42, 231, 26, 24, 37, 63, 176, 90, 135, 246, 252, 87, 77, 120, 253,
        50, 106, 60, 25, 68, 22, 63, 47, 255, 19, 182, 214, 78, 33, 148, 174, 210, 166, 244, 92, 147, 157, 102, 219,
        59, 4, 18, 29, 125, 130, 60, 250, 84, 68, 233, 1, 24, 246, 91, 16, 185, 193, 228, 69, 136, 177, 196, 234, 213,
        60, 40, 146, 218, 32, 131, 63, 160, 28, 205, 228, 178, 50, 248, 1, 189, 95, 12, 16, 109, 114, 100, 242, 102,
        229, 19, 185, 176, 246, 7, 162, 248, 49, 102, 160, 213, 28, 212, 253, 206, 187, 17, 230, 11, 41, 106, 238, 101,
        99, 133, 114, 61, 99, 102, 4, 121, 125, 64, 4, 2, 199, 135, 211, 135, 227, 159, 150, 160, 182, 68, 84, 51, 216,
        3, 177, 238, 50, 204, 181, 125, 187, 13, 114, 173, 20, 93, 11, 211, 207, 105, 24, 114, 86, 19, 20, 93, 7, 211,
        134, 171, 219, 163, 47, 178, 14, 251, 77, 195, 141, 32, 130, 98, 231, 219, 76, 171, 24, 73, 215, 56, 191, 107,
        0, 89, 137, 62, 0, 67, 195, 55, 30, 37, 31, 168, 89, 85, 55, 13, 145, 203, 170, 112, 174, 87, 44, 229, 220,
        135, 152, 18, 192, 107, 95, 169, 33, 252, 29, 87, 138, 179, 50, 186, 187, 171, 208, 74, 29, 59, 186, 90, 195,
        148, 16, 220, 0, 251, 135, 119, 169, 7, 234, 198, 14, 181, 86, 117, 33, 78, 169, 167, 141, 86, 248, 214, 110,
        245, 134, 116, 248, 23, 195, 35, 247, 47, 192, 40, 213, 34, 184, 221, 21, 211, 152, 20, 45, 80, 162, 233, 188,
        83, 148, 246, 37, 125, 26, 191, 216, 100, 4, 81, 126, 132, 126, 157, 224, 30, 78, 67, 41, 195, 38, 78, 70, 46,
        105, 45, 150, 77, 169, 26, 86, 136, 8, 231, 220, 107, 134, 107, 181, 144, 225, 208, 165, 57, 189, 129, 62, 98,
        245, 252, 129, 65, 32, 106, 14, 249, 108, 92, 246, 71, 185, 87, 184, 34, 113, 131, 45, 50, 54, 159, 2, 24, 132,
        85, 48, 238, 20, 31, 213, 188, 230, 190, 220, 28, 210, 105, 50, 103, 206, 106, 235, 57, 45, 14, 25, 190, 156,
        197, 8, 214, 126, 9, 80, 248, 241, 142, 218, 153, 154, 215, 0, 29, 18, 109, 251, 86, 139, 96, 172, 101, 221,
        109, 191, 164, 237, 183, 46, 26, 227, 67, 17, 137, 6, 120, 183, 235, 202, 62, 120, 173, 45, 166, 114, 209, 115,
        34, 210, 147, 15, 160, 160, 34, 197, 55, 53, 5, 124, 207, 188, 28, 78, 162, 61, 192, 207, 74, 54, 176, 66, 138,
        141, 121, 139, 195, 159, 248, 2, 92, 149, 161, 220, 75, 117, 255, 175, 73, 207, 227, 22, 128, 157, 101, 238,
        98, 12, 180, 174, 175, 240, 172, 176, 228, 243, 96, 217, 41, 76, 155, 218, 211, 70, 81, 63, 130, 32, 139, 209,
        117, 248, 22, 44, 17, 0, 185, 232, 210, 194, 249, 201, 229, 128, 68, 121, 234, 5, 169, 88, 104, 130, 164, 28,
        195, 255, 96, 113, 73, 231, 78, 74, 126, 25, 17, 162, 4, 106, 136, 77, 155, 115, 237, 183, 71, 245, 144, 197,
        71, 247, 147, 142, 89, 195, 170, 137, 163, 22, 130, 176, 182, 228, 45, 11, 126, 64, 223, 77, 5, 20, 126, 39,
        74, 39, 6, 145, 68, 128, 6, 199, 85, 107, 105, 122, 133, 39, 23, 12, 65, 126, 46, 158, 84, 21, 224, 66, 64,
        136, 173, 231, 3, 128, 15, 16, 153, 149, 202, 252, 237, 212, 242, 52, 235, 5, 172, 30, 141, 52, 175, 175, 193,
        194, 194, 78, 22, 24, 28, 208, 76, 72, 54, 190, 178, 33, 82, 231, 66, 150, 24, 195, 16, 239, 220, 46, 128, 31,
        142, 112, 199, 8, 86, 48, 39, 48, 109, 166, 100, 148, 67, 241, 243, 230, 56, 175, 109, 10, 225, 27, 104, 2, 58,
        229, 128, 243, 146, 94, 195, 93, 229, 31, 90, 40, 160, 18, 71, 113, 122, 109, 225, 15, 19, 80, 101, 171, 85,
        253, 183, 51, 177, 123, 187, 224, 16, 93, 131, 190, 250, 8, 5, 218, 119, 20, 85, 172, 17, 80, 45, 239, 82, 70,
        56, 83, 72, 238, 61, 24, 117, 166, 29, 39, 172, 22, 41, 117, 216, 25, 84, 129, 118, 232, 5, 71, 168, 206, 113,
        186, 174, 64, 35, 79, 183, 10, 36, 215, 121, 128, 163, 157, 201, 133, 195, 100, 55, 216, 173, 133, 110, 211,
        142, 164, 128, 133, 234, 194, 124, 167, 237, 145, 66, 195, 226, 226, 28, 38, 177, 143, 148, 6, 77, 80, 140, 58,
        227, 84, 122, 166, 250, 4, 36, 206, 142, 192, 215, 197, 81, 167, 74, 177, 248, 237, 22, 104, 249, 84, 100, 15,
        67, 160, 49, 46, 107, 82, 133, 237, 197, 213, 97, 99, 77, 63, 165, 120, 34, 0, 62, 123, 192, 80, 191, 69, 242,
        119, 109, 61, 248, 138, 8, 234, 59, 237, 243, 24, 253, 235, 21, 5, 78, 143, 56, 101, 102, 204, 131, 247, 105,
        246, 237, 158, 63, 16, 57, 189, 0, 95, 192, 121, 229, 97, 210, 234, 186, 52, 198, 233, 230, 49, 37, 48, 35, 6,
        9, 42, 134, 72, 134, 247, 13, 1, 9, 21, 49, 22, 4, 20, 27, 107, 46, 42, 0, 94, 6, 30, 93, 236, 25, 203, 28,
        110, 41, 54, 91, 172, 222, 174, 48, 49, 48, 33, 48, 9, 6, 5, 43, 14, 3, 2, 26, 5, 0, 4, 20, 133, 152, 125, 169,
        245, 196, 253, 186, 205, 99, 63, 182, 172, 81, 134, 19, 6, 254, 147, 125, 4, 8, 159, 119, 149, 49, 4, 224, 71,
        190, 2, 2, 8, 0,
    ]
}
