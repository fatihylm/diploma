<template>
  <div>
    <div class="header">
      <h1>Converter for CSV Files</h1>
    </div>
    <div class="header2">
      <h2>Data Converter</h2>
    </div>
    <div class="header3">
      <h3>Click on the Button to convert your File</h3>
    </div>
    <div class="main">
      <button @click="openFileInput">Upload File</button>
      <label for="file-input" id="dropzone" @click="openFileInput">
      </label>
      <input ref="fileInput" id="file-input" type="file" @change="handleFileChange" style="display: none;" single />
      <div class="file-list">
        <ul v-if="files.length > 0">
          <li v-for="(file, index) in files" :key="index">
            {{ file.name }}
          </li>
        </ul>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  data() {
    return {
      files: [],
    };
  },
  methods: {
    openFileInput() {
      this.$refs.fileInput.click();
    },
    handleFileChange(event) {
      const selectedFile = event.target.files[0];
      this.files = Array.from(event.target.files);
      if (selectedFile) {
        const fileName = selectedFile.name;
        const fileExtension = fileName.slice(((fileName.lastIndexOf(".") - 1) >>> 0) + 2);

        if (fileExtension === 'csv') {
          this.uploadFile(selectedFile);
        } else {
          alert('Please select a valid CSV file.');
          event.target.value = ''; // Clear the input field
        }
      }
    },
    async uploadFile(file) {
      let formData = new FormData();
      formData.append('file', file);

      try {
        const response = await fetch('/file_upload', {
          method: 'POST',
          body: formData,
        });
        const result = await response.json();
        if (response.ok) {
          alert('File successfully uploaded: ' + result.message);
        } else {
          alert('Error: ' + result.error);
        }
      } catch (error) {
        console.error('Error:', error);
        alert('Upload failed. Please try again.');
      }
    },
  },
};
</script>

<style>
@import '/src/css/DragDropStyle.css';
</style>
