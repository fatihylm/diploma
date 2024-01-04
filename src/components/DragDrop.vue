<template>
  <div>
    <div class="header">
      <h1>Click on the button to upload your file</h1>
    </div> 
    <button @click="openFileInput">Upload Files</button>
    <div class="main">
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
      this.files = Array.from(event.target.files);
    },
  },
};
</script>

<style>
@import '/src/css/DragDropStyle.css';
</style>
