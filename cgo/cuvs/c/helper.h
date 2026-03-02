#ifndef MO_CUVS_HELPER_H
#define MO_CUVS_HELPER_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Gets the number of available CUDA devices.
 * @return The number of devices, or a negative error code.
 */
int GpuGetDeviceCount();

/**
 * @brief Gets the list of available CUDA device IDs.
 * @param devices Pre-allocated array to store the device IDs.
 * @param max_count The maximum number of devices the array can hold.
 * @return The number of device IDs actually written to the array.
 */
int GpuGetDeviceList(int* devices, int max_count);

#ifdef __cplusplus
}
#endif

#endif // MO_CUVS_HELPER_H
